use std::io;
use std::io::ErrorKind;
use std::collections::HashMap;
use std::error::Error;
use rusqlite::Connection;
use uuid::Uuid;
use crate::snip::SnipError;

/// Analysis of the document derived from
#[derive(Debug)]
pub struct SnipAnalysis {
    pub words: Vec<SnipWord>,
}

impl SnipAnalysis {
    /// get vector positions of desired context including term position
    pub fn get_term_context_positions(&self, position: usize, count: usize) -> Vec<usize> {
        let mut context: Vec<usize> = Vec::new();
        let mut context_prefix: Vec<usize> = Vec::new();
        let mut context_suffix: Vec<usize> = Vec::new();
        // println!("term: \"{}\" position: {}", &self.words[position].word, position);

        // check bounds of context start
        let context_prefix_pos: usize = match position as i64 - count as i64 {
            x if x <= 0 => 0, // use position zero
            x => x as usize,
        };
        // println!("context_prefix_pos: {}", context_prefix_pos);

        // check bounds of context stop
        let context_suffix_pos: usize = match position + 1 {
            x if x > self.words.len() => self.words.len(),
            x => x,
        };
        // println!("context_suffix_pos: {}", context_suffix_pos);

        for (i, _) in self.words.iter().enumerate() {
            if i >= context_prefix_pos && i < position {
                context_prefix.push(i);
            }
            if i > position && i < context_suffix_pos + count {
                context_suffix.push(i);
            }
        }
        // println!("prefix: {:?}", context_prefix);
        context.append(&mut context_prefix);
        context.push(position);
        // println!("suffix: {:?}", context_suffix);
        context.append(&mut context_suffix);
        context
    }

    /// get document words corresponding to the given context positions
    pub fn get_term_context_words(&self, context: Vec<usize>) -> Vec<&SnipWord> {
        let mut words: Vec<String> = Vec::new();
        let mut snip_words: Vec<&SnipWord> = Vec::new();

        for pos in context {
            words.push(self.words[pos].word.clone());
            snip_words.push(&self.words[pos]);
        }
        snip_words
    }
}

/// Represents a word in the document, along with meta information derived from document analysis
#[derive(Debug)]
pub struct SnipWord {
    pub word: String,
    pub stem: String,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
    pub index: Option<WordIndex>,
}

#[derive(Debug)]
pub struct WordIndex {
    pub count: u64,
    pub positions: Vec<u64>,
    pub term: String,
}

impl WordIndex {
    pub fn positions_to_string(&self) -> String {
        let joined: Vec<String> = self.positions.iter().map(|x| x.to_string()).collect();
        joined.join(",")
    }

    pub fn positions_to_u64(pos: String) -> Result<Vec<u64>, Box<dyn Error>> {
        let split: Vec<String> = pos.split(',').map(|x| x.to_string()).collect();
        let mut output: Vec<u64> = Vec::new();
        for n in split {
            let n_u64: u64 = n.parse::<u64>()?;
            output.push(n_u64);
        }
        Ok(output)
    }
}

/// Returns ids of documents that match the given term
pub fn search_data(conn: &Connection, term: &String) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid FROM snip WHERE data LIKE :term")?;
    let term_fuzzy = format!("{} {}{}", "%", term, "%");

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

/// Search for term and return a vector containing uuid and vector of term positions
pub fn search_index_term(conn: &Connection, term: &String) -> Result<Vec<(Uuid, Vec<usize>)>, Box<dyn Error>> {
    let mut results: Vec<(Uuid, Vec<usize>)> = Vec::new();
    let mut results_single: (Uuid, Vec<usize>);
    let mut stmt = conn.prepare("SELECT uuid, positions FROM snip_index_rs WHERE term = :term")?;
    let rows = stmt.query_and_then(&[(":term", &term)], |row| -> Result<(String, String), Box<dyn Error>> {
        let id: String = row.get(0)?;
        let positions: String = row.get(1)?;
        Ok((id, positions))
    })?;

    // if let Some(row) = rows.into_iter().flatten().next() {
    for row in rows.flatten() {
        // parse uuid, split positions string and create vector
        let id: Uuid = Uuid::try_parse(row.0.as_str())?;
        let positions_split: Vec<usize> = row.1.split(',').map(|x| x.parse::<usize>().expect("converting position string to usize")).collect();
        results_single = (id, positions_split);
        results.push(results_single);
        // return Ok(results_single);
    }
    if results.is_empty() {
        return Err(Box::new(SnipError::SearchNoMatches("no matches found in index".to_string())));
    }
    Ok(results)
}

/// Search the index and return uuids that contain term
pub fn search_uuids_matching_term(conn: &Connection, term: String) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut ids: Vec<Uuid> = Vec::new();
    let mut stmt = conn.prepare("SELECT uuid FROM snip_index_rs WHERE term = :term")?;
    let rows = stmt.query_and_then(&[(":term", &term)], |row| -> Result<String, Box<dyn Error>> {
        let id: String = row.get(0)?;
        Ok(id)
    })?;

    for row in rows.flatten() {
        let id = Uuid::try_parse(row.as_str())?;
        ids.push(id);
    }
    Ok(ids)
}

/// Searches the database index returning UUIDs that match supplied terms
pub fn search_index_terms(conn: &Connection, terms: &Vec<String>) -> Result<HashMap<String, Vec<(Uuid, Vec<usize>)>>, Box<dyn Error>> {
    let mut results: HashMap<String, Vec<(Uuid, Vec<usize>)>> = HashMap::new();

    // search each term
    for term in terms {
        let results_single_term = search_index_term(conn, term)?;
        results.insert(term.clone(), results_single_term);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::error::Error;
    use uuid::Uuid;
    use crate::snip;
    use crate::snip::test_prep::*;

    #[test]
    fn test_get_term_context() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        let id = Uuid::try_parse(ID_STR)?;
        let mut s = snip::get_from_uuid(&conn, id)?;
        s.analyze()?;
        // println!("{}", s.text);

        let position = 3;
        let term = &s.analysis.words[position].word;
        let expect: Vec<usize> = vec![0, 1, 2, 3, 4, 5, 6];
        let context = s.analysis.get_term_context_positions(position, 3);
        println!("context: {:?}", context);
        assert_eq!(expect, context);

        // print context
        let context_full: Vec<&SnipWord> = s.analysis.get_term_context_words(context);
        for c in context_full.iter() {
            // print first word
            if c.word == *term {
                print!("[{}]", c.word);
            } else {
                print!("{}", c.word);
            }
            if let Some(suffix) = &c.suffix {
                print!("{}", suffix);
            }
        }

        Ok(())
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