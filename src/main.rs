use chrono::{DateTime, FixedOffset};
use clap::{arg, Command};
use rusqlite::{Connection, Result};
use rust_stemmers::{Algorithm, Stemmer};
use std::error::Error;
use std::{env, io};
use uuid::Uuid;

struct Snip {
    uuid: String,
    name: String,
    text: String,
    timestamp: DateTime<FixedOffset>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cmd = Command::new("snip-rs")
        .bin_name("snip-rs")
        .arg_required_else_help(true)
        .subcommand_required(true)
        .subcommand(clap::command!("ls").about("List all snips"))
        .subcommand(
            Command::new("stem")
                .about("Stem word from stdin")
                .arg(arg!(<word> "The word to stem"))
                .arg_required_else_help(true),
        )
        .subcommand(clap::command!("get").about("Print first snip in database"));

    let matches = cmd.get_matches();

    let db_file_default = ".snip.sqlite3".to_string();
    let home_dir = match env::var("HOME") {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };
    let db_path = env::var("SNIP_DB").unwrap_or(format!("{}/{}", home_dir, db_file_default));
    let conn = Connection::open(db_path)?;

    // process all subcommands as in: https://docs.rs/clap/latest/clap/_derive/_cookbook/git/index.html
    match matches.subcommand() {
        Some(("get", _)) => {
            let s = match get_first_snip(&conn) {
                Ok(v) => v,
                Err(e) => panic!("{}", e),
            };
            println!(
                "first snip: uuid: {} timestamp: {} name: {} text: {}",
                s.uuid, s.timestamp, s.name, s.text
            );
        }
        Some(("help", _)) => {
            println!("help");
        }
        Some(("ls", _)) => {
            list_snips(&conn).expect("could not list snips");
        }
        Some(("stem", sub_matches)) => {
            let term = match sub_matches.get_one::<String>("word") {
                Some(v) => v.to_owned(),
                None => read_data_from_stdin()?,
            };
            println!("{} -> {}", term, stem_something(&term));
        }
        _ => {
            println!("invalid subcommand");
        }
    }

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

        println!("{} {} {}", split_uuid(id)[0], s.timestamp, s.name);
    }

    Ok(())
}

fn stem_something(s: &str) -> String {
    let stemmer = Stemmer::create(Algorithm::English);
    stemmer.stem(s.to_lowercase().as_str()).to_string()
}

fn read_data_from_stdin() -> Result<String, io::Error> {
    let mut buffer = String::new();
    io::stdin().read_line(&mut buffer)?;
    Ok(buffer.trim_end().to_owned())
}

fn split_uuid(uuid: Uuid) -> Vec<String> {
    uuid.to_string().split('-').map(|s| s.to_string()).collect()
}
