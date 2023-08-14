use crate::snip;
use chrono::{DateTime, FixedOffset};
use rusqlite::Connection;
use rust_stemmers::Stemmer;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::io::Read;
use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;

use crate::snip::{Attachment, SnipAnalysis, SnipError, SnipWord, WordIndex};

/// Snip is the main struct representing a document.
pub struct Snip {
    pub uuid: Uuid,
    pub name: String,
    pub text: String,
    pub timestamp: DateTime<FixedOffset>,
    pub analysis: SnipAnalysis,
    pub attachments: Vec<Attachment>,
}

impl Snip {
    pub fn analyze(&mut self) -> Result<(), SnipError> {
        self.split_words()?;
        self.stem_words()?;
        self.scan_fragments()?;

        Ok(())
    }

    /// Removes all word indices for a document
    fn drop_word_indices(&self, conn: &Connection) -> Result<(), Box<dyn Error>> {
        let mut stmt = conn.prepare("DELETE FROM snip_index_rs WHERE uuid = :uuid")?;
        stmt.execute(&[(":uuid", &self.uuid.to_string())])?;
        Ok(())
    }

    /// Collects all attachments belonging to this document
    pub fn collect_attachments(&mut self, conn: &Connection) -> Result<(), Box<dyn Error>> {
        // clear current data from vector
        self.attachments.clear();
        let mut stmt =
            conn.prepare("SELECT uuid FROM snip_attachment WHERE snip_uuid = :snip_uuid")?;
        let query_iter = stmt.query_and_then(
            &[(":snip_uuid", &self.uuid.to_string())],
            |row| -> Result<String, rusqlite::Error> {
                let id_str = row.get(0)?;
                Ok(id_str)
            },
        )?;

        for row in query_iter.flatten() {
            let id = Uuid::try_parse(row.as_str())?;
            let a = snip::get_attachment_from_uuid(conn, id)?;
            self.attachments.push(a);
        }
        Ok(())
    }

    /// Returns the WordIndex of a given term within the document index
    fn _get_word_index(
        &self,
        conn: &Connection,
        term: &String,
    ) -> Result<WordIndex, Box<dyn Error>> {
        let mut stmt = conn.prepare(
            "SELECT count, positions FROM snip_index_rs WHERE uuid = :uuid AND term = :term",
        )?;
        let mut counter: usize = 0;
        let mut rows = stmt.query_map(
            &[(":uuid", &self.uuid.to_string()), (":term", term)],
            |row| {
                let count: u64 = row.get(0)?;
                let positions_string: String = row.get(1)?;
                let positions: Vec<u64> = positions_string
                    .split(',')
                    .map(|x| x.parse::<u64>().expect("error parsing u64 from string"))
                    .collect();
                println!("counter: {}", counter);
                counter += 1;

                Ok(WordIndex {
                    count,
                    positions,
                    term: term.clone(),
                })
            },
        )?;

        if let Some(i) = rows.next() {
            return match i {
                Ok(v) => Ok(v),
                Err(e) => Err(Box::new(e)),
            };
        }
        Err(Box::new(SnipError::UuidNotFound(
            "word not found".to_string(),
        )))
    }

    /// writes an index to the database for searching
    pub fn index(&mut self, conn: &Connection) -> Result<(), Box<dyn Error>> {
        // ensure that item has been analyzed
        if self.analysis.words.is_empty() {
            self.analyze()?;
        }

        // build counts of each term
        let mut terms: HashMap<String, u64> = HashMap::new();
        for word in &self.analysis.words {
            let count = terms.entry(word.stem.to_owned()).or_insert(1);
            *count += 1;
        }
        // println!("{:#?}", terms);

        // collect the positions of each term in the document
        let mut terms_positions: HashMap<String, Vec<u64>> = HashMap::new();
        for (pos, word) in self.analysis.words.iter().enumerate() {
            let positions = terms_positions
                .entry(word.stem.clone())
                .or_insert(Vec::new());
            positions.push(pos as u64);
        }
        // println!("{:#?}", terms_positions);

        self.drop_word_indices(conn)?;
        for pos in terms_positions {
            // insert this data
            // term: lorem count: 2 positions: "0,217"
            // println!("term: {} count: {} positions: {:#?}", pos.0, pos.1.len(), pos_joined);
            let index = WordIndex {
                count: pos.1.len() as u64,
                positions: pos.1,
                term: pos.0,
            };
            self.write_word_index(conn, index)?;
        }

        Ok(())
    }

    /// scans and assigns all prefix and suffix strings to all analyzed words
    pub fn scan_fragments(&mut self) -> Result<(), SnipError> {
        // scan the document for tokens, in order collecting surrounding data for each token
        let mut prefixes: Vec<Option<String>> = Vec::new();
        let mut suffixes: Vec<Option<String>> = Vec::new();
        let mut offset: usize = 0;
        let mut suffix_last: Option<String> = None;

        // Iterate through each of the words present.
        for word in self.analysis.words.windows(2) {
            let next_word = word.get(1);

            // default is to scan to the end of string unless there is a next word
            let mut offset_next = self.text.len();
            if let Some(next_word) = &next_word {
                offset_next = next_word.offset;
            }

            // PREFIX
            let mut prefix: Option<String> = None;
            let read_prefix_from: usize = offset;
            let read_prefix_to: usize = word[0].offset;

            // The previous suffix should always be present, except at the start of the first iteration.
            if let Some(s) = suffix_last.clone() {
                prefix = Some(s);
            } else {
                let prefix_slice = self.text.get(read_prefix_from..read_prefix_to);
                if let Some(p) = prefix_slice {
                    prefix = Some(p.to_string());
                }
            }
            prefixes.push(prefix);

            // SUFFIX
            let mut suffix: Option<String> = None;
            let read_suffix_from: usize = word[0].offset + word[0].word.len();
            let read_suffix_to: usize = offset_next;

            let suffix_slice = self.text.get(read_suffix_from..read_suffix_to);
            if let Some(s) = suffix_slice {
                suffix_last = Some(s.to_string());
                suffix = suffix_last.clone();
            }
            suffixes.push(suffix);

            // println!("word: {} word.offset: {}", word[1].word, word[1].offset);
            offset = word[0].offset + word[0].word.len(); // set offset to after the current word
        }
        // push last suffix
        if let Some(last_word) = self.analysis.words.last() {
            let read_from = last_word.offset + last_word.word.len();
            if let Some(last_suffix) = self.text.get(read_from..) {
                suffixes.push(Some(last_suffix.to_string()));
            }
        }

        // assign prefixes
        for (i, prefix) in prefixes.iter().enumerate() {
            self.analysis.words[i].prefix = prefix.to_owned();
        }
        // assign suffixes
        for (i, suffix) in suffixes.iter().enumerate() {
            self.analysis.words[i].suffix = suffix.to_owned();
        }

        Ok(())
    }

    /// Splits the document text and writes words the the analysis
    pub fn split_words(&mut self) -> Result<(), SnipError> {
        let words = self
            .text
            .unicode_word_indices()
            .collect::<Vec<(usize, &str)>>();

        for (offset, word) in words {
            // create DocWord
            let word_analyzed = SnipWord {
                word: word.to_string(),
                stem: String::new(),
                prefix: None, // these are scanned later
                suffix: None, // these are scanned later
                index: None,  // this is built later
                offset,
            };
            self.analysis.words.push(word_analyzed);
        }
        Ok(())
    }

    /// Stems the document words and writes the stems to the analysis.
    fn stem_words(&mut self) -> Result<(), SnipError> {
        let stemmer = Stemmer::create(rust_stemmers::Algorithm::English);

        for word_analyzed in self.analysis.words.iter_mut() {
            let word_tmp = word_analyzed.word.to_lowercase().clone();

            // Most stemmers require apostrophe in ASCII for compatibility. While we
            // make the transformation here so that stems are generated correctly, we
            // want to avoid changing the original data.
            let word_tmp = word_tmp.replace('’', "'");

            let stem = stemmer.stem(word_tmp.as_str());
            word_analyzed.stem = stem.to_string();
        }
        Ok(())
    }

    /// Writes all fields to the database, overwriting existing data
    pub fn update(&self, conn: &Connection) -> Result<(), Box<dyn Error>> {
        let mut stmt = conn.prepare("UPDATE snip SET (data, timestamp, name) = (:data, :timestamp, :name) WHERE uuid = :uuid")?;
        let _ = stmt.execute(&[
            (":data", &self.text.to_string()),
            (":timestamp", &self.timestamp.to_rfc3339()),
            (":name", &self.name.to_string()),
            (":uuid", &self.uuid.to_string()),
        ])?;
        Ok(())
    }

    /// Writes an index for a word to the database for searching
    fn write_word_index(
        &mut self,
        conn: &Connection,
        word: WordIndex,
    ) -> Result<(), Box<dyn Error>> {
        let mut stmt = conn.prepare("INSERT OR REPLACE INTO snip_index_rs(term, uuid, count, positions) VALUES (:term, :uuid, :count, :positions)")?;
        let positions_string = word.positions_to_string();
        let count = word.count;
        let result = stmt.execute(&[
            (":term", &word.term),
            (":uuid", &self.uuid.to_string()),
            (":count", &count.to_string()),
            (":positions", &positions_string),
        ])?;

        if result != 1 {
            return Err(Box::new(SnipError::General(
                "no rows were updated".to_string(),
            )));
        }
        Ok(())
    }
}

/// Clear the search index
pub fn clear_index(conn: &Connection) -> Result<usize, Box<dyn Error>> {
    let mut stmt = conn.prepare("DELETE FROM snip_index_rs")?;
    let n = stmt.execute([])?;
    Ok(n)
}

/// Create the main tables used to store documents, attachments, and document matrix.
pub fn create_snip_tables(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare(
        "CREATE TABLE IF NOT EXISTS snip(uuid TEXT, timestamp TEXT, name TEXT, data TEXT)",
    )?;
    stmt.raw_execute()?;

    let mut stmt = conn.prepare("CREATE TABLE IF NOT EXISTS snip_attachment(uuid TEXT, snip_uuid TEXT, timestamp TEXT, name TEXT, data BLOB, size INTEGER)")?;
    stmt.raw_execute()?;

    let mut stmt = conn.prepare("CREATE TABLE IF NOT EXISTS snip_index(term TEXT, uuid TEXT, count INTEGER, positions TEXT)")?;
    stmt.raw_execute()?;

    let mut stmt = conn.prepare("CREATE TABLE IF NOT EXISTS snip_index_rs(term TEXT, uuid TEXT, count INTEGER, positions TEXT)")?;
    stmt.raw_execute()?;

    Ok(())
}

/// Create the table used to index documents for full text search. This is only done when the table is not present.
pub fn create_index_table(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare("CREATE TABLE IF NOT EXISTS snip_index_rs(term TEXT, uuid TEXT, count INTEGER, positions TEXT)")?;
    stmt.raw_execute()?;

    Ok(())
}

/// Returns the grapheme index at which `word` is fully matched within the containing `text`.
pub fn find_by_graph(word: &str, text: Vec<&str>) -> Option<usize> {
    let word_graphs: Vec<&str> = word.graphemes(true).collect();
    let mut match_buf: Vec<&str> = Vec::new();

    let mut cur = 0;
    for (i, c) in text.iter().enumerate() {
        if *c == word_graphs[cur] {
            match_buf.push(c);
            let word_compare: String = match_buf.concat();

            // check whole word
            if word_compare == word {
                return Some(i - cur);
            }
            cur += 1;
        } else {
            match_buf.clear();
            cur = 0;
        }
    }
    None
}

/// Generate document name from provided text
pub fn generate_name(text: &String, count: usize) -> Result<String, Box<dyn Error>> {
    let mut name = String::new();

    let words: Vec<&str> = text.unicode_words().collect();

    // ensure at least one word is present
    if words.is_empty() {
        return Err(Box::new(SnipError::General(
            "The document has no words, so a name cannot be generated.".to_string(),
        )));
    }

    if words.len() < count {
        return Ok(words.join(" ").to_string());
    }

    // return desired count
    for (i, word) in words.iter().enumerate() {
        if i == count {
            break;
        }
        name = format!("{name}{word} ");
    }
    // remove last space
    name = name.trim_end().to_string();

    Ok(name)
}

/// Get the snip specified matching the given full-length uuid string
pub fn get_from_uuid(conn: &Connection, id: &Uuid) -> Result<Snip, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid, timestamp, name, data FROM snip WHERE uuid = :id")?;
    let rows = stmt.query_and_then(&[(":id", &id.to_string())], |row| {
        snip_from_db(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)
    })?;

    if let Some(s) = rows.into_iter().flatten().next() {
        return Ok(s);
    }
    Err(Box::new(SnipError::UuidNotFound(id.to_string())))
}

/// Indexes the terms of all documents in the database
pub fn index_all_items(conn: &Connection) -> Result<(), Box<dyn Error>> {
    // clear first
    clear_index(conn)?;

    // iterate through snips
    let mut stmt = conn.prepare("SELECT uuid, timestamp, name, data FROM snip")?;
    let rows = stmt.query_and_then([], |row| {
        snip_from_db(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)
    })?;

    for snip in rows {
        let mut s = snip.unwrap();
        match s.analyze() {
            Ok(_) => (),
            Err(e) => return Err(Box::new(e)),
        }
        s.index(conn)?;
    }
    Ok(())
}

/// Adds a new document to the database
pub fn insert_snip(conn: &Connection, s: &Snip) -> Result<(), Box<dyn Error>> {
    let mut stmt =
        conn.prepare("INSERT INTO snip(uuid, timestamp, name, data) VALUES (?1, ?2, ?3, ?4)")?;
    stmt.execute([
        s.uuid.to_string(),
        s.timestamp.to_rfc3339(),
        s.name.clone(),
        s.text.clone(),
    ])?;

    Ok(())
}

/// Return a vector of Uuid of all documents in the database
pub fn uuid_list(conn: &Connection, limit: usize) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut ids: Vec<Uuid> = Vec::new();

    if limit != 0 {
        let mut stmt =
            conn.prepare("SELECT uuid FROM snip ORDER BY datetime(timestamp) DESC LIMIT :limit")?;
        let query_iter = stmt.query_map(&[(":limit", &limit)], |row| {
            let id_str: String = row.get(0)?;
            Ok(id_str)
        })?;

        for id_str in query_iter.flatten() {
            let id = Uuid::try_parse(id_str.as_str())?;
            ids.push(id);
        }
    } else {
        let mut stmt = conn.prepare("SELECT uuid FROM snip ORDER BY datetime(timestamp) DESC")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let id_str: String = row.get(0)?;
            let id = Uuid::try_parse(id_str.as_str())?;
            ids.push(id);
        }
    }

    Ok(ids)
}

/// Read all data from standard input, line by line, and return it as a String.
pub fn read_lines_from_stdin() -> Result<String, Box<dyn Error>> {
    let mut data = String::new();
    io::stdin().read_to_string(&mut data)?;
    Ok(data)
}

/// Remove a document matching given uuid
pub fn remove_snip(conn: &Connection, id: Uuid) -> Result<(), Box<dyn Error>> {
    let mut s = get_from_uuid(conn, &id)?;
    // collect and remove attachments
    s.collect_attachments(conn)?;
    for a in &s.attachments {
        a.remove(conn)?;
    }

    // remove terms from the index
    s.drop_word_indices(conn)?;

    // remove the document
    let mut stmt = conn.prepare("DELETE FROM snip WHERE uuid = ?1")?;
    let n = stmt.execute([id.to_string()]);
    match n {
        Ok(n) if n == 1 => Ok(()),
        _ => Err(Box::new(SnipError::General(
            "delete did not return a singular result".to_string(),
        ))),
    }
}

/// Returns a Snip struct parsed from the database
///
/// By default, the document is returned without attachments collected. This is for
/// performance reasons, as many operations require no attachment knowledge.
fn snip_from_db(
    id: String,
    ts: String,
    name: String,
    text: String,
) -> Result<Snip, Box<dyn Error>> {
    let timestamp = match DateTime::parse_from_rfc3339(ts.as_str()) {
        Ok(v) => v,
        Err(e) => return Err(Box::new(e)),
    };

    let uuid = match Uuid::try_parse(id.as_str()) {
        Ok(v) => v,
        Err(e) => return Err(Box::new(e)),
    };

    Ok(Snip {
        uuid,
        name,
        timestamp,
        text,
        analysis: SnipAnalysis { words: vec![] },
        attachments: Vec::new(),
    })
}

pub fn split_uuid(uuid: &Uuid) -> Vec<String> {
    uuid.to_string().split('-').map(|s| s.to_string()).collect()
}

pub fn strip_punctuation(s: &str) -> &str {
    let chars_strip = &['.', ',', '!', '?', '"', '\'', '[', ']', '(', ')'];

    let mut clean = match s.strip_prefix(chars_strip) {
        Some(v) => v,
        None => s,
    };
    clean = match clean.strip_suffix(chars_strip) {
        Some(v) => v,
        None => clean,
    };
    clean
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snip::get_attachment_from_uuid;
    use crate::snip::test_prep::*;

    #[test]
    fn test_collect_attachments() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        let id = Uuid::try_parse(ID_STR)?;
        let mut s = get_from_uuid(&conn, &id)?;
        assert_eq!(s.attachments.len(), 0);

        s.collect_attachments(&conn)?;
        assert_eq!(s.attachments.len(), 1); // expect one attachment
                                            // repeat the test to ensure that document refreshes properly
        s.collect_attachments(&conn)?;
        assert_eq!(s.attachments.len(), 1); // expect one attachment
        Ok(())
    }

    #[test]
    fn test_generate_name() -> Result<(), Box<dyn Error>> {
        let text = "Documents are good: for the soul, and other things as well.".to_string();
        let expect = "Documents are good for the soul".to_string();

        let result = generate_name(&text, 6)?;
        if result != expect {
            panic!("expected '{}', got '{}'", expect, result);
        }

        Ok(())
    }

    #[test]
    fn test_get_from_uuid() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        let id = Uuid::try_parse(ID_STR).expect("parsing uuid from static string");

        let _s = get_from_uuid(&conn, &id)?;
        Ok(())
    }

    #[test]
    fn test_index_snip() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        let id = Uuid::try_parse(ID_STR)?;
        let mut s = get_from_uuid(&conn, &id)?;

        s.index(&conn)?;
        // check data
        let mut stmt = conn.prepare("SELECT term, count, positions from snip_index_rs WHERE uuid = :uuid AND term = 'lorem'")?;
        let rows = stmt.query_and_then(
            &[(":uuid", &id.to_string())],
            |row| -> Result<WordIndex, Box<dyn Error>> {
                let term: String = row.get(0)?;
                let count: u64 = row.get(1)?;
                let positions_str: String = row.get(2)?;
                let positions = WordIndex::positions_to_u64(positions_str)?;
                let s = WordIndex {
                    term,
                    count,
                    positions,
                };
                Ok(s)
            },
        )?;

        for data in rows {
            let d = data.unwrap();
            if d.count != 2 {
                return Err(Box::new(SnipError::General(
                    format!("expected count {}, got {}", 2, d.count).to_string(),
                )));
            }
            let positions_expect: Vec<u64> = vec![0, 217];
            if d.positions != positions_expect {
                return Err(Box::new(SnipError::General(
                    format!(
                        "expected positions {:?}, got {:?}",
                        positions_expect, d.positions
                    )
                    .to_string(),
                )));
            }
            // println!("test_index_snip -> term: {} count: {} positions: {:?}", d.term, d.count, d.positions);
        }

        Ok(())
    }

    #[test]
    fn test_insert_snip() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        let id = Uuid::new_v4();

        let s = Snip {
            name: "Test".to_string(),
            uuid: id,
            timestamp: chrono::Local::now().fixed_offset(),
            text: "Test Data".to_string(),
            analysis: SnipAnalysis { words: Vec::new() },
            attachments: Vec::new(),
        };
        insert_snip(&conn, &s)?;

        // verify
        let mut stmt = conn.prepare("SELECT uuid FROM snip WHERE uuid = ?")?;
        let mut rows = stmt.query([id.to_string()])?;
        while let Some(row) = rows.next()? {
            let id_str: String = row.get(0)?;
            let id_check: Uuid = match Uuid::parse_str(id_str.as_str()) {
                Ok(v) => v,
                Err(e) => panic!("{}", e),
            };
            assert_eq!(id, id_check);
        }

        Ok(())
    }

    #[test]
    fn test_remove_snip() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        let id = Uuid::try_parse(ID_STR)?;
        let attachment_id = Uuid::try_parse(ID_ATTACH_STR)?;
        remove_snip(&conn, id)?;

        // verify attachment was deleted
        if get_attachment_from_uuid(&conn, attachment_id).is_ok() {
            return Err(Box::new(SnipError::General(
                "attachment is still present after snip deletion call".to_string(),
            )));
        }

        // verify document was deleted
        if get_from_uuid(&conn, &id).is_ok() {
            return Err(Box::new(SnipError::General(
                "document is still present after attempted deletion".to_string(),
            )));
        }
        Ok(())
    }

    #[test]
    fn test_update() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database()?;
        let id = Uuid::try_parse(ID_STR)?;

        // this is the data that will be written
        let expect = Snip {
            uuid: id,
            name: "Test Name".to_string(),
            text: "Test Text".to_string(),
            timestamp: chrono::Local::now().fixed_offset(),
            analysis: SnipAnalysis { words: Vec::new() }, // dynamic data, not database
            attachments: Vec::new(),                      // dynamic data, not database
        };
        expect.update(&conn)?;

        // verify
        let s = get_from_uuid(&conn, &id)?;

        assert_eq!(expect.name, s.name);
        assert_eq!(expect.text, s.text);
        assert_eq!(expect.timestamp, s.timestamp);

        Ok(())
    }
}
