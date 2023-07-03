use chrono::{DateTime, FixedOffset};
use rusqlite::{Connection, Result};
use rust_stemmers::{Algorithm, Stemmer};
use std::error::Error;
use std::{env, io};
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug)]
struct Snip {
    uuid: String,
    name: String,
    text: String,
    timestamp: DateTime<FixedOffset>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let db_file_default = ".snip.sqlite3".to_string();

    let home_dir = match env::var("HOME") {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };
    let db_path = env::var("SNIP_DB").unwrap_or(format!("{}/{}", home_dir, db_file_default));
    let conn = Connection::open(db_path)?;

    match list_snips(&conn) {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    }

    let word = read_data_from_stdin()?.trim_end().to_string();
    let stem = stem_something(&word);
    println!("Stem word: {} -> {}", word, stem);
    let s_first = match get_first_snip(&conn) {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };
    println!("first snip: {:?}", s_first);

    Ok(())
}

fn get_first_snip(conn: &Connection) -> Result<Snip, Box<dyn Error>> {
    let mut stmt = match conn.prepare("SELECT uuid, name, timestamp, data FROM snip LIMIT 1") {
        Ok(v) => v,
        Err(e) => return Err(Box::new(e)),
    };

    let mut query_iter = stmt.query_map([], |row| {
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

    if let Some(s) = query_iter.next() {
        return Ok(s.unwrap());
    }

    Err(Box::new(std::io::Error::new(
        io::ErrorKind::NotFound,
        "damn",
    )))
}

fn list_snips(conn: &Connection) -> Result<(), Box<dyn Error>> {
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

        print!("{} - ", chrono::Utc::now());
        println!("{} {} {}", split_uuid(id)[0], s.timestamp, s.name);
    }

    Ok(())
}

fn stem_something(s: &str) -> String {
    let stemmer = Stemmer::create(Algorithm::English);
    stemmer.stem(s.to_lowercase().as_str()).to_string()
}

#[allow(dead_code)]
fn read_data_from_stdin() -> Result<String, io::Error> {
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    Ok(buffer)
}

fn split_uuid(uuid: Uuid) -> Vec<String> {
    uuid.to_string().split('-').map(|s| s.to_string()).collect()
}
