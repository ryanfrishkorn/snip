use chrono;
use rusqlite::{Connection, Result};
use rust_stemmers::{Algorithm, Stemmer};
use std::error::Error;
use std::{env, io};
use uuid::Uuid;

struct Snip {
    uuid: String,
    name: String,
    timestamp: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let db_file_default = ".snip.sqlite3".to_string();

    let home_dir = match env::var("HOME") {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };
    let db_path = env::var("SNIP_DB").unwrap_or(format!("{}/{}", home_dir, db_file_default));
    let conn = Connection::open(db_path)?;

    let mut stmt = match conn.prepare("SELECT uuid, name, timestamp from snip") {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };

    let query_iter = stmt.query_map([], |row| {
        Ok(Snip {
            uuid: row.get(0)?,
            name: row.get(1)?,
            timestamp: row.get(2)?,
        })
    })?;

    for snip in query_iter {
        let s = snip.unwrap();
        let id = match Uuid::parse_str(&s.uuid) {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };
        print!("{} - ", chrono::Utc::now());
        println!("{} {} {}", split_uuid(id)[0], s.timestamp, s.name);
    }

    let word = read_data_from_stdin()?.trim_end().to_string();
    let stem = stem_something(&word);
    print!("Stem word: {} -> {}\n", word, stem);

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
