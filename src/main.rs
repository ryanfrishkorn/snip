pub mod snip;

use clap::{Arg, ArgAction, Command};
use rusqlite::{Connection, OpenFlags, Result};
use rust_stemmers::{Algorithm, Stemmer};
use std::error::Error;
use std::{env};
use unicode_segmentation::UnicodeSegmentation;

fn main() -> Result<(), Box<dyn Error>> {
    let cmd = Command::new("snip-rs")
        .bin_name("snip-rs")
        .arg_required_else_help(true)
        .arg(Arg::new("read-only")
            .long("read-only")
            .action(ArgAction::SetTrue)
        )
        .subcommand_required(true)
        .subcommand(
            Command::new("get")
                .about("Get from uuid")
                .arg_required_else_help(true)
                .arg(Arg::new("uuid"))
        )
        .subcommand(
            Command::new("index")
                .about("Reindex the database")
                .arg_required_else_help(false)
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
                .arg_required_else_help(true)
                .arg(Arg::new("terms")
                    .action(ArgAction::Append)
                    .required(true)
                )
        )
        .subcommand(
            Command::new("split")
                .about("Split a string into words")
                .arg_required_else_help(false)
                .arg(Arg::new("string"))
        )
        .subcommand(
            Command::new("stem")
                .about("Stem word from stdin")
                .arg_required_else_help(false)
                .arg(Arg::new("words"))
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

    // process all subcommands as in: https://docs.rs/clap/latest/clap/_derive/_cookbook/git/index.html
    match matches.subcommand() {
        Some(("get", sub_matches)) => {
            let id_str = match sub_matches.get_one::<String>("uuid") {
                Some(v) => v,
                None => panic!("{}", "need uuid"),
            };
            // search for unique uuid to allow partial string arg
            let id_str_full = match snip::search_uuid(&conn, id_str) {
                Ok(v) => v,
                Err(e) => panic!("{}", e),
            };
            let s = match snip::get_from_uuid(&conn, &id_str_full.to_string()) {
                Ok(v) => v,
                Err(e) => panic!("{}", e),
            };
            // print header
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
        Some(("help", _)) => {
            println!("help");
        }
        Some(("ls", _)) => {
            // honor arguments if present
            if let Some(arg_matches) = matches.subcommand_matches("ls") {
                snip::list_snips(&conn, arg_matches.get_flag("l"), arg_matches.get_flag("t")).expect("could not list snips");
            } else {
                // default no args
                snip::list_snips(&conn, false, false).expect("could not list snips");
            }
        }
        Some(("search", sub_matches)) => {
            if let Some(args) = sub_matches.get_many::<String>("terms") {
                let terms: Vec<String> = args.map(|x| x.to_string()).collect();
                println!("terms: {:?}", terms);
                for term in terms {
                    let _ = snip::search_data(&conn, &term);
                }
            }
        }
        Some(("stem", sub_matches)) => {
            let input = match sub_matches.get_one::<String>("words") {
                Some(v) => v.to_owned(),
                None => snip::read_lines_from_stdin(),
            };
            let words = input.unicode_words().collect::<Vec<&str>>();
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
                None => snip::read_lines_from_stdin(),
            };
            let words = input.unicode_words();
            println!("{:?}", words.collect::<Vec<&str>>());
        }
        Some(("index", _sub_matches)) => {
            snip::create_index_table(&conn)?;
            snip::index_all_items(&conn)?;
        }
        _ => {
            eprintln!("subcommand processing error");
            std::process::exit(1);
        }
    }

    Ok(())
}
