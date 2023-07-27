pub mod snip;

use clap::{Arg, ArgAction, Command};
use colored::*;
use rusqlite::{Connection, OpenFlags, Result};
use rust_stemmers::{Algorithm, Stemmer};
use snip::{Snip, SnipAnalysis};
use std::env;
use std::error::Error;
use std::io::Read;
use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;
use crate::snip::get_from_uuid;

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
            Command::new("attach")
                .about("Attach binary data to document")
                .subcommand_required(true)
                .subcommand(
                    Command::new("ls")
                        .about("list attachments")
                        .arg_required_else_help(false)
                        .arg(
                            Arg::new("long")
                                .short('l')
                                .num_args(0)
                                .action(ArgAction::SetTrue),
                        )
                        .arg(
                            Arg::new("size")
                                .short('s')
                                .num_args(0)
                                .action(ArgAction::SetTrue),
                        )
                        .arg(
                            Arg::new("time")
                                .short('t')
                                .num_args(0)
                                .action(ArgAction::SetTrue),
                        ),
                ),
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
        Err(e) => panic!("Could not obtain HOME env: {}", e),
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
        let name = sub_matches
            .get_one::<String>("name")
            .ok_or("matching name arg")?;
        let mut text: String = String::new();
        match sub_matches.get_one::<String>("file") {
            Some(v) => text = std::fs::read_to_string(v)?,
            None => {
                std::io::stdin().read_to_string(&mut text)?; // FIXME I don't like this
            }
        };

        // create document
        let mut s = Snip {
            uuid: Uuid::new_v4(),
            name: name.to_owned(),
            timestamp: chrono::Local::now().fixed_offset(),
            text,
            analysis: SnipAnalysis { words: vec![] },
        };

        snip::insert_snip(&conn, &s)?;
        s.index(&conn)?;
        println!("added uuid: {}", s.uuid);
    }

    // ATTACH
    if let Some(("attach", sub_matches)) = matches.subcommand() {
        // ATTACH LS
        if let Some(("ls", attach_sub_matches)) = sub_matches.subcommand() {
            let ids = snip::get_attachment_all(&conn)?;
            for id in ids {
                let a = snip::get_attachment_from_uuid(&conn, id)?;

                // uuid
                if attach_sub_matches.get_flag("long") {
                    print!("{} ", a.uuid);
                } else {
                    print!("{} ", snip::split_uuid(a.uuid)[0]);
                }

                // timestamp
                if attach_sub_matches.get_flag("time") {
                    print!("{} ", a.timestamp);
                }

                // size
                if attach_sub_matches.get_flag("size") {
                    print!("{:9} ", a.size);
                }

                // name
                print!("{}", a.name);
                println!();
            }
        }
    }

    // GET
    if let Some(("get", sub_matches)) = matches.subcommand() {
        let id_str = sub_matches
            .get_one::<String>("uuid")
            .ok_or("uuid not present")?;

        // search for unique uuid to allow partial string arg
        let id = snip::search_uuid(&conn, id_str)?;
        let mut s = snip::get_from_uuid(&conn, id)?;

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
            // TODO remove duplicate search terms if supplied

            // search for all terms and print
            let results = snip::search_index_terms(&conn, &terms_stem)?;
            for term in terms_stem.iter() {
                let (id, positions) = results.get(term.as_str()).ok_or("error parsing results from hashmap")?;
                // println!("uuid: {} positions: {:?}", id, positions);

                // retrieve and analyze document to obtain context
                let mut s = get_from_uuid(&conn, *id)?;
                s.analyze()?;
                // let context = s.analysis.get_term_context_words(positions.to_owned());
                println!("{}", s.name);
                println!("  {} [{}: {}]", snip::split_uuid(s.uuid)[0], term, positions.len());

                for pos in positions {
                    // gather context indices and print them
                    let context = s.analysis.get_term_context_positions(*pos, 8);
                    // print!("    [0-0] \"");
                    let position_first = context.first().ok_or("finding first context position")?;
                    let position_last = context.last().ok_or("finding last context position")?;
                    print!("    [{}-{}] \"", position_first, position_last);
                    for (i, p) in context.iter().enumerate() {
                        let snip_word = &s.analysis.words[*p];
                        // check for matching word
                        match &snip_word.stem {
                            x if x.to_lowercase() == *term => print!("{}", snip_word.word.red()),
                            _ => print!("{}", snip_word.word),
                        }
                        // print!("{}", snip_word.word);
                        // if let Some(suffix) = &s.analysis.words[idx].suffix {
                        if let Some(suffix) = &snip_word.suffix {
                            if i == context.len() - 1 { // do not print the final suffix
                                break;
                            }
                            // TODO remove repetitive whitespace to conform formatted text to search results
                            print!("{}", suffix.replace('\n', " ")); // no newlines
                        }
                    }
                    println!("\"");
                }
                println!();
            }

            /*
            // single term direct data search
            for (i, term) in terms_stem.iter().enumerate() {
                let results = snip::search_data(&conn, term)?;
                println!("results ({}): {:?}", terms[i], results);
            }
             */
        }
    }

    // STEM
    if let Some(("stem", sub_matches)) = matches.subcommand() {
        let input = match sub_matches.get_one::<String>("words") {
            Some(v) => v.to_owned(),
            None => snip::read_lines_from_stdin()?,
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
            None => snip::read_lines_from_stdin()?,
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
