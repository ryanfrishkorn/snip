use std::io;
use std::io::ErrorKind;
use std::collections::HashMap;
use std::error::Error;
use rusqlite::Connection;
use uuid::Uuid;

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

pub fn search_index_term(conn: &Connection, term: &String) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut results: Vec<Uuid> = Vec::new();
    let mut stmt = conn.prepare("SELECT uuid FROM snip_index_rs WHERE term = :term")?;
    let rows = stmt.query_and_then(&[(":term", &term)], |row| -> Result<String, Box<dyn Error>> {
        let id: String = row.get(0)?;
        Ok(id)
    })?;

    for id_str in rows.flatten() {
        let id: Uuid = Uuid::try_parse(id_str.as_str())?;
        results.push(id);
    }
    Ok(results)
}


/// Searches the database index returning UUIDs that match supplied terms
pub fn search_index_terms(conn: &Connection, terms: Vec<String>) -> Result<HashMap<String, Vec<Uuid>>, Box<dyn Error>> {
    let mut results: HashMap<String, Vec<Uuid>> = HashMap::new();

    // search each term
    for term in terms {
        let result_single = search_index_term(conn, &term)?;
        results.insert(term, result_single);
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
    use crate::snip::test_prep::*;

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