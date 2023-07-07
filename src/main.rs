use chrono::{DateTime, FixedOffset};
use clap::{Arg, ArgAction, Command};
use regex::Regex;
use rusqlite::{Connection, Result};
use rust_stemmers::{Algorithm, Stemmer};
use std::error::Error;
use std::{env, io};
use std::io::{ErrorKind, Read};
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
        .subcommand(
            Command::new("get")
                .about("Get from uuid")
                .arg(Arg::new("uuid"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("index")
                .about("Reindex the database")
                .arg_required_else_help(false),
        )
        .subcommand(
            Command::new("ls")
                .about("List all snips")
                .arg(Arg::new("l")
                    .short('l')
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                )
                .arg(Arg::new("t")
                    .short('t')
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                )
        )
        .subcommand(
            Command::new("search")
                .about("Search for terms")
                .arg(Arg::new("terms")
                    .action(ArgAction::Append)
                    .required(true)
                )
                .arg_required_else_help(true)
        )
        .subcommand(
            Command::new("split")
                .about("Split a string into words")
                .arg(Arg::new("string"))
                .arg_required_else_help(false)
        )
        .subcommand(
            Command::new("stem")
                .about("Stem word from stdin")
                .arg(Arg::new("words"))
                .arg_required_else_help(false),
        );


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
        Some(("get", sub_matches)) => {
            let id_str = match sub_matches.get_one::<String>("uuid") {
                Some(v) => v,
                None => panic!("{}", "need uuid"),
            };
            // search for unique uuid to allow partial string arg
            let id_str_full = match search_uuid(&conn, id_str) {
                Ok(v) => v,
                Err(e) => panic!("{}", e),
            };
            let s = match get_from_uuid(&conn, &id_str_full.to_string()) {
                Ok(v) => v,
                Err(e) => panic!("{}", e),
            };
            println!(
                "uuid: {}\nname: {}\ntimestamp: {}\n----\n{}\n----",
                s.uuid, s.name, s.timestamp, s.text
            );
        }
        Some(("help", _)) => {
            println!("help");
        }
        Some(("ls", _)) => {
            // honor arguments if present
            if let Some(arg_matches) = matches.subcommand_matches("ls") {
                list_snips(&conn, arg_matches.get_flag("l"), arg_matches.get_flag("t")).expect("could not list snips");
            } else {
                // default no args
                list_snips(&conn, false, false).expect("could not list snips");
            }
        }
        Some(("search", sub_matches)) => {
            if let Some(args) = sub_matches.get_many::<String>("terms") {
                let terms = args.map(|x| x.as_str()).collect::<Vec<&str>>();
                println!("terms: {:?}", terms);
            }
        }
        Some(("stem", sub_matches)) => {
            let input = match sub_matches.get_one::<String>("words") {
                Some(v) => v.to_owned(),
                None => read_lines_from_stdin(),
            };
            let words = split_words(&input);
            let stemmer = Stemmer::create(Algorithm::English);
            for (i, w) in words.iter().enumerate() {
                print!("{}", stemmer.stem(w.to_lowercase().as_str()));

                // newline on last term
                if words.len() - 1 == i {
                    println!();
                } else {
                    print!(" ");
                }
            }
            eprintln!("words: {}", words.len());
        }
        Some(("split", sub_matches)) => {
            let input = match sub_matches.get_one::<String>("string") {
                Some(v) => v.to_owned(),
                None => read_lines_from_stdin(),
            };
            println!(
                "{:?}",
                split_words(&input)
                    .into_iter()
                    .map(|x| x.to_lowercase())
                    .collect::<Vec<String>>()
            );
        }
        Some(("index", _sub_matches)) => {
            create_index_table(&conn)?;
            index_all_items(&conn)?;
        }
        _ => {
            eprintln!("subcommand processing error");
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Create the table used to index documents for full text search. This is only done when the table is not present.
fn create_index_table(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("CREATE TABLE IF NOT EXISTS snip_index_rs(term TEXT, uuid TEXT, count INTEGER, positions TEXT)")?;
    stmt.raw_execute()?;

    Ok(())
}

/// Get the snip specified matching the given full-length uuid string.
fn get_from_uuid(conn: &Connection, id_str: &str) -> Result<Snip, Box<dyn Error>> {
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

fn index_all_items(conn: &Connection) -> Result<(), Box<dyn Error>> {
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

fn index_item(_conn: &Connection, _s: &Snip) -> Result<(), Box<dyn Error>> {
    Ok(())
}

/// Print a list of all documents in the database.
fn list_snips(conn: &Connection, full_uuid: bool, show_time: bool) -> Result<(), Box<dyn Error>> {
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
fn read_lines_from_stdin() -> String {
    let mut data = String::new();

    match io::stdin().read_to_string(&mut data) {
        Ok(_) => (),
        Err(e) => panic!("{}", e),
    }
    data
}

/// Search for a uuid matching the supplied partial string.
/// The partial uuid must match a unique record to return the result.
fn search_uuid(conn: &Connection, id_partial: &str) -> Result<Uuid, Box<dyn Error>> {
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

fn split_uuid(uuid: Uuid) -> Vec<String> {
    uuid.to_string().split('-').map(|s| s.to_string()).collect()
}

/// Split a string and into a vector of words delimited by whitespace. No punctuation is not stripped.
fn split_words(s: &str) -> Vec<&str> {
    let input = s.trim_start().trim_end();

    let pattern = Regex::new(r"(?m)\s+").unwrap();
    pattern.split(input).collect()
}

#[allow(dead_code)]
fn strip_punctuation(s: &str) -> &str {
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

    const DB_PATH: &str = "snip.enwiki.partial.sqlite3";
    const ID_STR: &str = "ba652e2d-b248-4bcc-b36e-c26c0d0e8002";

    #[test]
    fn split_mutiline_string() -> Result<()> {
        let s = r#"Lorem ipsum (dolor) sit amet, consectetur
second line?

that was an [empty] line.
"#;
        let expect: Vec<&str> = vec![
            "Lorem",
            "ipsum",
            "(dolor)",
            "sit",
            "amet,",
            "consectetur",
            "second",
            "line?",
            "that",
            "was",
            "an",
            "[empty]",
            "line.",
        ];
        let split = split_words(s);
        assert_eq!(expect, split);
        Ok(())
    }

    #[test]
    fn test_get_from_uuid() -> Result<()> {
        let conn = Connection::open(DB_PATH)?;

        if let Ok(s) = get_from_uuid(&conn, ID_STR) {
            println!("{} {} {}", s.uuid, s.timestamp, s.name);
            return Ok(());
        }
        panic!("{}", "could not get snip from uuid");
    }

    #[test]
    fn test_search_uuid() -> Result<()> {
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
        let conn = Connection::open(DB_PATH)?;

        // test all uuid string partials
        for p in &partials {
            let id = search_uuid(&conn, p.0);
            match id {
                Ok(v) => assert_eq!(expect, v),
                Err(e) => panic!("{}, full: {}, partial: {}", e, ID_STR, &p.0),
            }
        }
        Ok(())
    }
}
