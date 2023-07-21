use chrono::{DateTime, FixedOffset};
use rusqlite::Connection;
use rust_stemmers::Stemmer;
use std::error::Error;
use std::fmt;
use std::io;
use std::io::{ErrorKind, Read};
use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;

#[derive(Debug)]
/// Snip is the main struct representing a document.
pub struct Snip {
    pub uuid: Uuid,
    pub name: String,
    pub text: String,
    pub timestamp: DateTime<FixedOffset>,
    pub analysis: SnipAnalysis,
}

/// Analysis of the document derived from
#[derive(Debug)]
pub struct SnipAnalysis {
    pub words: Vec<SnipWord>,
}

/// Represents a word in the document, along with meta information derived from document analysis
#[derive(Debug)]
pub struct SnipWord {
    pub word: String,
    pub stem: String,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

impl Snip {
    pub fn analyze(&mut self) -> Result<(), SnipError> {
        self.split_words()?;
        self.stem_words()?;
        self.scan_fragments()?;

        Ok(())
    }

    /// scans and assigns all prefix and suffix strings to all analyzed words
    pub fn scan_fragments(&mut self) -> Result<(), SnipError> {
        // scan the document for tokens, in order collecting surrounding data for each token
        let mut fragments: Vec<(usize, usize)> = Vec::new();
        let mut prefixes: Vec<Option<String>> = Vec::new();
        let mut suffix: Option<String> = None; // the suffix of the last word
        let mut offset: usize = 0; // last position of cursor
        let text_graphs = self.text.graphemes(true).collect::<Vec<_>>();

        for (i, word) in self.analysis.words.iter().enumerate() {
            // scan characters until word is encountered
            // println!("finding word: {} len: {} offset: {}", word.word, word.word.len(), offset);
            // start search from offset
            let mut cur = 0;

            // find index of word
            let text_slice: Vec<&str> = match text_graphs.get(offset..) {
                Some(v) => v.to_vec(),
                None => break, // no more data left
            };

            let offset_match = match find_by_graph(&word.word, text_slice) {
                Some(v) => v,
                None => break, // we must be at the end here
            };
            // println!("match cursor offset: {}", offset_match);
            let fragment_pre_len = offset_match - cur;
            // println!("fragment_pre_len: {}", fragment_pre_len);

            let mut prefix: Option<String> = None;
            // len > 0 means that a prefix exists
            if fragment_pre_len > 0 {
                // read the prefix length characters
                let mut prefix_buf: Vec<String> = Vec::new();
                for (i, c) in text_graphs[offset..].iter().enumerate() {
                    if i < cur {
                        // println!("skip");
                        continue;
                    }
                    if i < fragment_pre_len {
                        // println!("cursor: {} push: {}", cur, *c);
                        prefix_buf.push(c.to_string());
                        cur += 1;
                    } else {
                        break;
                    }
                }
                // println!("prefix_buf: \"{}\"", prefix_buf);
                prefix = Some(prefix_buf.concat());
            }
            prefixes.push(prefix);

            // build fragment
            let frag = (offset, offset + cur);
            fragments.push(frag);

            // println!("iteration: {} offset: {} word_len: {} cursor: {} word: {}", i, offset, word.word.graphemes(true).count(), cur, word.word);
            offset = offset + cur + word.word.graphemes(true).count(); // set offset to current cursor value

            // LAST ITERATION
            if i == self.analysis.words.len() - 1 {
                // offset less than length indicates a suffix remains
                if offset < text_graphs.len() {
                    let mut suffix_buf: Vec<String> = Vec::new();
                    for c in text_graphs[offset..].iter() {
                        suffix_buf.push(c.to_string());
                    }
                    suffix = match suffix_buf.is_empty() {
                        true => None,
                        false => Some(suffix_buf.iter().map(|x| x.to_owned()).collect::<String>()),
                    };
                    // println!("final suffix: {:?}", suffix);
                }
            }
        }

        // assign prefixes
        for (i, prefix) in prefixes.iter().enumerate() {
            self.analysis.words[i].prefix = prefix.to_owned();
            if i > 0 {
                // set previous suffix to the current prefix
                self.analysis.words[i - 1].suffix = prefix.to_owned();
            }
        }

        // assign last suffix
        self.analysis.words[prefixes.len() - 1].suffix = suffix;
        Ok(())
    }

    /// Splits the document text and writes words the the analysis
    pub fn split_words(&mut self) -> Result<(), SnipError> {
        let words = self
            .text
            .unicode_words()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        for word in words {
            // create DocWord
            let word_analyzed = SnipWord {
                word,
                stem: String::new(),
                prefix: None, // these are scanned later
                suffix: None, // these are scanned later
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
            let stem = stemmer.stem(word_tmp.as_str());
            word_analyzed.stem = stem.to_string();
        }
        Ok(())
    }
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

    Ok(())
}

/// Create the table used to index documents for full text search. This is only done when the table is not present.
pub fn create_index_table(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare("CREATE TABLE IF NOT EXISTS snip_index_rs(term TEXT, uuid TEXT, count INTEGER, positions TEXT)")?;
    stmt.raw_execute()?;

    Ok(())
}

// Returns the character index of a fully matched word
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

/// Get the snip specified matching the given full-length uuid string.
pub fn get_from_uuid(conn: &Connection, id: Uuid) -> Result<Snip, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid, timestamp, name, data FROM snip WHERE uuid = :id")?;
    let rows = stmt.query_and_then(&[(":id", &id.to_string())], |row| {
        snip_from_db(row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)
    })?;

    if let Some(s) = rows.into_iter().flatten().next() {
        return Ok(s);
    }
    Err(Box::new(SnipError::UuidNotFound(id.to_string())))
}

pub fn index_all_items(conn: &Connection) -> Result<(), Box<dyn Error>> {
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
        index_item(conn, &s)?;
    }

    // perform analysis
    // write to database index
    Ok(())
}

pub fn index_item(_conn: &Connection, _s: &Snip) -> Result<(), SnipError> {
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

/// Print a list of all documents in the database.
pub fn list_snips(
    conn: &Connection,
    full_uuid: bool,
    show_time: bool,
) -> Result<(), Box<dyn Error>> {
    let mut stmt = match conn.prepare("SELECT uuid, name, timestamp, data from snip") {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };

    let query_iter = stmt.query_map([], |row| {
        // parse timestamp
        let ts: String = row.get(2)?;
        let ts_parsed = match DateTime::parse_from_rfc3339(ts.as_str()) {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };

        let id_str: String = row.get(0)?;
        let id = match Uuid::try_parse(id_str.as_str()) {
            Ok(v) => v,
            Err(e) => panic!("parsing uuid: {}", e),
        };

        Ok(Snip {
            uuid: id,
            name: row.get(1)?,
            timestamp: ts_parsed,
            text: row.get(3)?,
            analysis: SnipAnalysis { words: vec![] },
        })
    })?;

    for snip in query_iter {
        let s = snip.unwrap();

        // uuid
        match full_uuid {
            true => print!("{} ", s.uuid),
            false => print!("{} ", split_uuid(s.uuid)[0]),
        }
        // timestamp
        if show_time {
            print!("{} ", s.timestamp);
        }
        // name
        print!("{} ", s.name);
        println!(); // just for the newline
                    // println!("{} {} {}", split_uuid(id)[0], s.timestamp, s.name);
    }

    Ok(())
}

/// Read all data from standard input, line by line, and return it as a String.
pub fn read_lines_from_stdin() -> String {
    let mut data = String::new();

    match io::stdin().read_to_string(&mut data) {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    }
    data
}

/// Remove a document matching given uuid
pub fn remove_snip(conn: &Connection, id: Uuid) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare("DELETE FROM snip WHERE uuid = ?1")?;
    let n = stmt.execute([id.to_string()]);
    match n {
        Ok(n) if n == 1 => Ok(()),
        _ => Err(Box::new(io::Error::new(
            ErrorKind::Other,
            "delete did not return a singular result",
        ))),
    }
}

/// Returns ids of documents that match the given term
pub fn search_data(conn: &Connection, term: &String) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid FROM snip WHERE data LIKE :term")?;
    let term_fuzzy = format!("{} {} {}", "%", term, "%");

    let query_iter = stmt.query_map(&[(":term", &term_fuzzy)], |row| {
        let id_str: String = row.get(0)?;
        Ok(id_str)
    })?;

    let mut results: Vec<Uuid> = Vec::new();
    for i in query_iter {
        let id_str = match i {
            Ok(v) => v,
            Err(e) => return Err(Box::new(e)),
        };
        match Uuid::parse_str(&id_str) {
            Ok(v) => results.push(v),
            Err(e) => return Err(Box::new(e)),
        }
    }
    // println!("results: {:?}", results);
    Ok(results)
}

/// Search for a uuid matching the supplied partial string.
/// The partial uuid must match a unique record to return the result.
pub fn search_uuid(conn: &Connection, id_partial: &str) -> Result<Uuid, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid from snip WHERE uuid LIKE :id LIMIT 2")?;
    let id_partial_fuzzy = format!("{}{}{}", "%", id_partial, "%");

    let query_iter = stmt.query_map(&[(":id", &id_partial_fuzzy)], |row| {
        let id_str = row.get(0)?;
        Ok(id_str)
    })?;

    // return only if a singular result is matched
    let mut id_found = "".to_string();
    let mut first_run = true;
    let err_not_found = Box::new(io::Error::new(
        ErrorKind::NotFound,
        "could not find unique uuid match",
    ));
    for id in query_iter {
        if first_run {
            first_run = false;
            id_found = id.unwrap();
        } else {
            return Err(err_not_found);
        }
    }

    if !id_found.is_empty() {
        return match Uuid::parse_str(&id_found) {
            Ok(v) => Ok(v),
            Err(e) => Err(Box::new(e)),
        };
    }
    Err(err_not_found)
}

// Returns a Snip struct parsing from database values
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
    })
}

pub fn split_uuid(uuid: Uuid) -> Vec<String> {
    uuid.to_string().split('-').map(|s| s.to_string()).collect()
}

#[allow(dead_code)]
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

/// Errors for Snip Analysis
pub enum SnipError {
    Analysis(String),
    UuidNotFound(String),
}

impl Error for SnipError {}

impl fmt::Display for SnipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnipError::Analysis(s) => write!(f, "Analysis encountered an error: {}", s),
            SnipError::UuidNotFound(s) => write!(f, "uuid {} was not found", s),
        }
    }
}

impl fmt::Debug for SnipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnipError::Analysis(s) => write!(
                f,
                "{{ SnipError::Analysis({}) file: {}, line: {} }}",
                s,
                file!(),
                line!()
            ),
            SnipError::UuidNotFound(s) => write!(
                f,
                "{{ SnipError::UuidNotFound({}) file: {}, line: {} }}",
                s,
                file!(),
                line!()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::DatabaseName;
    use std::collections::HashMap;

    const ID_STR: &str = "ba652e2d-b248-4bcc-b36e-c26c0d0e8002";

    // This prepares an in-memory database for testing. This avoids database file name collisions
    // and allows each unit test to use congruent data yet be completely isolated. This function
    // panics to keep test function calls brief, and they cannot proceed unless it succeeds.
    fn prepare_database() -> Result<Connection, ()> {
        let conn = match Connection::open_in_memory() {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };
        // import data
        create_snip_tables(&conn).expect("creating database tables");
        import_snip_data(&conn).expect("importing test data");

        Ok(conn)
    }

    fn import_snip_data(conn: &Connection) -> Result<(), Box<dyn Error>> {
        let snip_file = "test_data/snip.csv";
        let snip_attachment_file = "test_data/snip_attachment.csv";

        let mut data = csv::Reader::from_path(snip_file)?;
        for r in data.records() {
            let record = r?;

            // gather record data
            let id = record.get(0).expect("getting uuid field");
            let timestamp = record.get(1).expect("getting timestamp field");
            let name = record.get(2).expect("getting name field");
            let data = record.get(3).expect("getting data field");

            // insert the record
            let mut stmt = conn.prepare("INSERT INTO snip(uuid, timestamp, name, data) VALUES (:id, :timestamp, :name, :data)")?;
            stmt.execute(&[
                (":id", id),
                (":timestamp", timestamp),
                (":name", name),
                (":data", data),
            ])?;
        }

        data = csv::Reader::from_path(snip_attachment_file)?;
        for r in data.records() {
            let record = r?;

            let id = record.get(0).expect("getting attachment uuid field");
            let snip_id = record.get(1).expect("getting uuid field");
            let timestamp = record.get(2).expect("getting timestamp field");
            let name = record.get(3).expect("getting name field");
            let size = record.get(4).expect("getting size field");

            // use name to read data from test file
            let data = std::fs::read(format!("{}/{}", "test_data/attachments/", name))?;
            let data = data.as_slice();

            let mut stmt = conn.prepare("INSERT INTO snip_attachment(uuid, snip_uuid, timestamp, name, data, size) VALUES (:id, :snip_id, :timestamp, :name, ZEROBLOB(:blob_size), :size)")?;
            stmt.execute(&[
                (":id", id),
                (":snip_id", snip_id),
                (":timestamp", timestamp),
                (":name", name),
                (":blob_size", data.len().to_string().as_str()),
                (":size", size),
            ])?;
            let row_id = conn.last_insert_rowid();

            // add binary data to blob
            let mut blob =
                conn.blob_open(DatabaseName::Main, "snip_attachment", "data", row_id, false)?;
            blob.write_at(data, 0)?;
        }

        Ok(())
    }

    #[test]
    fn test_get_from_uuid() -> Result<(), ()> {
        let conn = prepare_database().expect("preparing in-memory database");

        if let Ok(s) = get_from_uuid(
            &conn,
            Uuid::try_parse(ID_STR).expect("parsing uuid from static string"),
        ) {
            println!("{} {} {}", s.uuid, s.timestamp, s.name);
            return Ok(());
        }
        panic!("{}", "could not get snip from uuid");
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
        remove_snip(&conn, id)?;

        // verify it was deleted
        match get_from_uuid(&conn, id) {
            Ok(_) => Err(Box::new(io::Error::new(
                ErrorKind::Other,
                "id is still present in database after attempted delete",
            ))),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn test_search_uuid() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");

        let partials: HashMap<String, String> = HashMap::from([
            // ba652e2d-b248-4bcc-b36e-c26c0d0e8002
            (ID_STR[0..8].to_string(), "segment 1".to_string()), // ba652e2d
            (ID_STR[9..13].to_string(), "segment 2".to_string()), // _________b248
            (ID_STR[14..18].to_string(), "segment 3".to_string()), // ______________4bbc
            (ID_STR[19..23].to_string(), "segment 4".to_string()), // ___________________b36e
            (ID_STR[24..].to_string(), "segment 5".to_string()), // ________________________c26c0d0e8002
            (ID_STR[7..12].to_string(), "partial 1".to_string()), // _______d-b24
            (ID_STR[7..14].to_string(), "partial 2".to_string()), // _______d-b248-
            (ID_STR[7..15].to_string(), "partial 3".to_string()), // _______d-b248-4
            (ID_STR[8..19].to_string(), "partial 4".to_string()), // ________-b248-4bcc-
            (ID_STR[23..].to_string(), "partial 5".to_string()), // _______________________-c26c0d0e8002
        ]);

        /*
        println!("ba652e2d-b248-4bcc-b36e-c26c0d0e8002");
        for p in &partials {
            println!("{} {}", p.0, p.1);
        }
        */

        let expect = match Uuid::parse_str(ID_STR) {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };

        // test all uuid string partials
        for p in &partials {
            println!("search uuid string: {}", p.0);
            let id = search_uuid(&conn, p.0);
            match id {
                Ok(v) => assert_eq!(expect, v),
                Err(e) => panic!("{}, full: {}, partial: {}", e, ID_STR, &p.0),
            }
        }
        Ok(())
    }
}
