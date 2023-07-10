use chrono::{DateTime, FixedOffset};
use rusqlite::{Connection};
use std::error::Error;
use std::{io};
use std::io::{ErrorKind, Read};
use uuid::Uuid;

/// Snip is the main struct representing a document.
pub struct Snip {
    pub uuid: String,
    pub name: String,
    pub text: String,
    pub timestamp: DateTime<FixedOffset>,
}

/// Create the main tables used to store documents, attachments, and document matrix.
pub fn create_snip_tables(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare("CREATE TABLE IF NOT EXISTS snip(uuid TEXT, timestamp TEXT, name TEXT, data TEXT)")?;
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

/// Get the snip specified matching the given full-length uuid string.
pub fn get_from_uuid(conn: &Connection, id_str: &str) -> Result<Snip, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid, timestamp, name, data FROM snip WHERE uuid = :id")?;

    let query_iter = stmt.query_map(&[(":id", &id_str)], |row| {
        let ts: String = row.get(1)?;
        // let ts_parsed = DateTime::parse_from_rfc3339(ts.as_str())?;
        let ts_parsed;
        match DateTime::parse_from_rfc3339(ts.as_str()) {
            Ok(v) => ts_parsed = v,
            Err(e) => {
                panic!("{}", e)
            }
        };

        Ok(Snip {
            uuid: row.get(0)?,
            name: row.get(2)?,
            timestamp: ts_parsed,
            text: row.get(3)?,
        })
    })?;

    if let Some(s) = query_iter.flatten().next() {
        return Ok(s);
    }

    Err(Box::new(io::Error::new(
        ErrorKind::NotFound,
        "not found",
    )))
}

pub fn index_all_items(conn: &Connection) -> Result<(), Box<dyn Error>> {
    // iterate through snips
    let mut stmt = conn.prepare("SELECT uuid, timestamp, name, data FROM snip")?;

    let query_iter = stmt.query_map([], |row| {
        let ts: String = row.get(1)?;
        // let ts_parsed = DateTime::parse_from_rfc3339(ts.as_str())?;
        let ts_parsed;
        match DateTime::parse_from_rfc3339(ts.as_str()) {
            Ok(v) => ts_parsed = v,
            Err(e) => {
                println!("ts: {}", ts);
                panic!("{}", e)
            }
        };

        Ok(Snip {
            uuid: row.get(0)?,
            name: row.get(2)?,
            timestamp: ts_parsed,
            text: row.get(3)?,
        })
    })?;

    for snip in query_iter {
        let s = snip.unwrap();
        index_item(conn, &s)?;
    }

    // obtain stem
    // perform analysis
    // write to new database index
    Ok(())
}

pub fn index_item(_conn: &Connection, _s: &Snip) -> Result<(), Box<dyn Error>> {
    Ok(())
}

/// Print a list of all documents in the database.
pub fn list_snips(conn: &Connection, full_uuid: bool, show_time: bool) -> Result<(), Box<dyn Error>> {
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

        Ok(Snip {
            uuid: row.get(0)?,
            name: row.get(1)?,
            timestamp: ts_parsed,
            text: row.get(3)?,
        })
    })?;

    for snip in query_iter {
        let s = snip.unwrap();
        let id = Uuid::parse_str(&s.uuid)?;

        // uuid
        match full_uuid {
            true => print!("{} ", s.uuid),
            false => print!("{} ", split_uuid(id)[0]),
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

pub fn search_data(conn: &Connection, term: &String) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid FROM snip WHERE data LIKE :term")?;
    let term_fuzzy = format!("{}{}{}", "%", term, "%");

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
    println!("results: {:?}", results);
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
    let err_not_found = Box::new(io::Error::new(ErrorKind::NotFound, "could not find unique uuid match"));
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
        }
    }
    Err(err_not_found)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use rusqlite::DatabaseName;
    use unicode_segmentation::UnicodeSegmentation;

    const ID_STR: &str = "ba652e2d-b248-4bcc-b36e-c26c0d0e8002";

    #[test]
    fn split_multi_line_string() -> Result<(), Box<dyn Error>> {
        let s = r#"Lorem ipsum (dolor) sit amet, consectetur
second line?

that was an [empty] line.
"#;
        let expect: Vec<&str> = vec![
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetur",
            "second",
            "line",
            "that",
            "was",
            "an",
            "empty",
            "line",
        ];
        let split: Vec<&str> = s.unicode_words().collect();
        assert_eq!(expect, split);
        Ok(())
    }

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
            let timestamp = record.get(1).expect("getting uuid field");
            let name = record.get(2).expect("getting uuid field");
            let data = record.get(3).expect("getting uuid field");

            // insert the record
            let mut stmt = conn.prepare("INSERT INTO snip(uuid, timestamp, name, data) VALUES (:id, :timestamp, :name, :data)")?;
            stmt.execute(&[(":id", id), (":timestamp", timestamp), (":name", name), (":data", data)])?;
        }

        data = csv::Reader::from_path(snip_attachment_file)?;
        for r in data.records() {
            let record = r?;

            let id = record.get(0).expect("getting attachment uuid field");
            let snip_id = record.get(1).expect("getting attachment uuid field");
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
            let mut blob = conn.blob_open(DatabaseName::Main, "snip_attachment", "data", row_id, false)?;
            blob.write_at(data, 0)?;
        }

        Ok(())
    }

    #[test]
    fn test_get_from_uuid() -> Result<(), ()> {
        let conn = prepare_database().expect("preparing in-memory database");

        if let Ok(s) = get_from_uuid(&conn, ID_STR) {
            println!("{} {} {}", s.uuid, s.timestamp, s.name);
            return Ok(());
        }
        panic!("{}", "could not get snip from uuid");
    }

    #[test]
    fn test_search_uuid() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");

        let partials: HashMap<String, String> = HashMap::from([    // ba652e2d-b248-4bcc-b36e-c26c0d0e8002
            (ID_STR[0..8].to_string(), "segment 1".to_string()),   // ba652e2d
            (ID_STR[9..13].to_string(), "segment 2".to_string()),  // _________b248
            (ID_STR[14..18].to_string(), "segment 3".to_string()), // ______________4bbc
            (ID_STR[19..23].to_string(), "segment 4".to_string()), // ___________________b36e
            (ID_STR[24..].to_string(), "segment 5".to_string()),   // ________________________c26c0d0e8002
            (ID_STR[7..12].to_string(), "partial 1".to_string()),  // _______d-b24
            (ID_STR[7..14].to_string(), "partial 2".to_string()),  // _______d-b248-
            (ID_STR[7..15].to_string(), "partial 3".to_string()),  // _______d-b248-4
            (ID_STR[8..19].to_string(), "partial 4".to_string()),  // ________-b248-4bcc-
            (ID_STR[23..].to_string(), "partial 5".to_string()),   // _______________________-c26c0d0e8002
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
