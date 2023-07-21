pub mod snip;

use clap::{Arg, ArgAction, Command};
use rusqlite::{Connection, OpenFlags, Result};
use rust_stemmers::{Algorithm, Stemmer};
use snip::{Snip, SnipAnalysis};
use std::env;
use std::error::Error;
use std::io::Read;
use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;

fn main() -> Result<(), Box<dyn Error>> {
    let cmd = Command::new("snip-rs")
        .bin_name("snip-rs")
        .arg_required_else_help(true)
        .arg(
            Arg::new("read-only")
                .long("read-only")
                .action(ArgAction::SetTrue),
        )
        .subcommand_required(true)
        .subcommand(
            Command::new("add")
                .about("Add new snip to database")
                .arg_required_else_help(false)
                .arg(Arg::new("file").short('f').long("file").num_args(1))
                .arg(Arg::new("name").short('n').long("name").num_args(1)),
        )
        .subcommand(
            Command::new("get")
                .about("Get from uuid")
                .arg_required_else_help(true)
                .arg(Arg::new("uuid"))
                .arg(
                    Arg::new("analyze")
                        .long("analyze")
                        .short('a')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("raw")
                        .long("raw")
                        .short('r')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("index")
                .about("Reindex the database")
                .arg_required_else_help(false),
        )
        .subcommand(
            Command::new("ls")
                .about("List all snips")
                .arg(
                    Arg::new("l")
                        .short('l')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("t")
                        .short('t')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove items")
                .arg_required_else_help(true)
                .arg(Arg::new("ids").action(ArgAction::Append).required(true)),
        )
        .subcommand(
            Command::new("search")
                .about("Search for terms")
                .arg_required_else_help(true)
                .arg(Arg::new("terms").action(ArgAction::Append).required(true)),
        )
        .subcommand(
            Command::new("split")
                .about("Split stdin into words")
                .arg_required_else_help(false)
                .arg(Arg::new("string")),
        )
        .subcommand(
            Command::new("stem")
                .about("Stem word from stdin")
                .arg_required_else_help(false)
                .arg(Arg::new("words")),
        );

    let matches = cmd.get_matches();
    let db_file_default = ".snip.sqlite3".to_string();
    let home_dir = match env::var("HOME") {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    };
    let db_path = env::var("SNIP_DB").unwrap_or(format!("{}/{}", home_dir, db_file_default));

    let conn = match matches.get_flag("read-only") {
        true => Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?,
        false => Connection::open(db_path)?,
    };
    // ensure that tables are present for basic functionality
    snip::create_snip_tables(&conn)?;

    // process all subcommands as in: https://docs.rs/clap/latest/clap/_derive/_cookbook/git/index.html
    // ADD
    if let Some(("add", sub_matches)) = matches.subcommand() {
        let name = match sub_matches.get_one::<String>("name") {
            Some(v) => v,
            None => panic!("{}", "provide a name"),
        };
        let mut text: String = String::new();
        match sub_matches.get_one::<String>("file") {
            Some(v) => text = std::fs::read_to_string(v)?,
            None => {
                std::io::stdin().read_to_string(&mut text)?; // FIXME I don't like this
            }
        };

        // create document
        let s = Snip {
            uuid: Uuid::new_v4(),
            name: name.to_owned(),
            timestamp: chrono::Local::now().fixed_offset(),
            text,
            analysis: SnipAnalysis { words: vec![] },
        };

        snip::insert_snip(&conn, &s)?;
        println!("added uuid: {}", s.uuid);
    }

    // GET
    if let Some(("get", sub_matches)) = matches.subcommand() {
        let id_str = match sub_matches.get_one::<String>("uuid") {
            Some(v) => v,
            None => panic!("{}", "need uuid"),
        };
        // search for unique uuid to allow partial string arg
        let id = match snip::search_uuid(&conn, id_str) {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };
        let mut s = match snip::get_from_uuid(&conn, id) {
            Ok(v) => v,
            Err(e) => panic!("{}", e),
        };

        // check for raw or formatted output
        if let Some(raw) = sub_matches.get_one::<bool>("raw") {
            match raw {
                // raw output
                true => print!("{}", s.text),
                // formatted output
                false => {
                    println!(
                        "uuid: {}\nname: {}\ntimestamp: {}\n----",
                        s.uuid, s.name, s.timestamp
                    );

                    // add a newline if not already present
                    match s.text.chars().last() {
                        Some(v) if v == '\n' => println!("{}----", s.text),
                        _ => println!("{}\n----", s.text),
                    }
                }
            }
        }

        if let Some(analyze) = sub_matches.get_one::<bool>("analyze") {
            if *analyze {
                // analyze
                match s.analyze() {
                    Ok(_) => (),
                    Err(e) => return Err(Box::new(e)),
                }
                println!("{:#?}\n", s.analysis);
            }
        }
    }

    // HELP
    if let Some(("help", _)) = matches.subcommand() {
        println!("help");
    }

    // LS
    if let Some(("ls", _)) = matches.subcommand() {
        // honor arguments if present
        if let Some(arg_matches) = matches.subcommand_matches("ls") {
            snip::list_snips(&conn, arg_matches.get_flag("l"), arg_matches.get_flag("t"))
                .expect("could not list snips");
        } else {
            // default no args
            snip::list_snips(&conn, false, false).expect("could not list snips");
        }
    }

    // RM
    if let Some(("rm", sub_matches)) = matches.subcommand() {
        if let Some(args) = sub_matches.get_many::<String>("ids") {
            // convert to uuid
            let ids_str: Vec<String> = args.map(|x| x.to_string()).collect();
            for (i, id_str) in ids_str.iter().enumerate() {
                // obtain full id
                let id = snip::search_uuid(&conn, id_str)?;
                snip::remove_snip(&conn, id)?;
                println!("{}/{} removed {}", i + 1, ids_str.len(), id);
            }
        }
    }

    // SEARCH
    if let Some(("search", sub_matches)) = matches.subcommand() {
        if let Some(args) = sub_matches.get_many::<String>("terms") {
            let terms: Vec<String> = args.map(|x| x.to_owned()).collect();
            let terms_stem = stem_vec(terms.clone());
            println!("terms: {:?}", terms_stem);
            for (i, term) in terms_stem.iter().enumerate() {
                let results = snip::search_data(&conn, term)?;
                println!("results ({}): {:?}", terms[i], results);
            }
        }
    }

    // STEM
    if let Some(("stem", sub_matches)) = matches.subcommand() {
        let input = match sub_matches.get_one::<String>("words") {
            Some(v) => v.to_owned(),
            None => snip::read_lines_from_stdin(),
        };
        let words = input.unicode_words().collect::<Vec<&str>>();
        let stemmer = Stemmer::create(Algorithm::English);
        let mut stems: Vec<String> = Vec::new();
        for w in words.iter() {
            stems.push(stemmer.stem(w.to_lowercase().as_str()).to_string());
        }
        println!("{:?}", stems);
    }

    // SPLIT
    if let Some(("split", sub_matches)) = matches.subcommand() {
        let input = match sub_matches.get_one::<String>("string") {
            Some(v) => v.to_owned(),
            None => snip::read_lines_from_stdin(),
        };
        let words = input.unicode_words();
        println!("{:?}", words.collect::<Vec<&str>>());
    }

    // INDEX
    if let Some(("index", _)) = matches.subcommand() {
        snip::create_index_table(&conn)?;
        snip::index_all_items(&conn)?;
    }

    Ok(())
}

fn stem_vec(words: Vec<String>) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);
    words.iter().map(|w| stemmer.stem(w).to_string()).collect()
}
