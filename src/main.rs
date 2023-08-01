pub mod snip;

use clap::{Arg, ArgAction, Command};
use colored::*;
use rusqlite::{Connection, OpenFlags, Result};
use rust_stemmers::{Algorithm, Stemmer};
use snip::{Snip, SnipAnalysis};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::Read;
use std::path::Path;
use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;
use snip_rs::SnipError;

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
                    Command::new("add")
                        .about("add file to document")
                        .arg_required_else_help(true)
                        .arg(
                            Arg::new("snip_uuid")
                                .num_args(1)
                        )
                        .arg(
                            Arg::new("files")
                                .action(ArgAction::Append)
                        )
                )
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
                )
                .subcommand(
                    Command::new("rm")
                        .about("remove attachments")
                        .arg_required_else_help(true)
                        .arg(
                            Arg::new("uuids")
                                .action(ArgAction::Append)
                        )
                )
                .subcommand(
                    Command::new("write")
                        .about("write attachment to local file")
                        .arg_required_else_help(true)
                        .arg(
                            Arg::new("id")
                                .num_args(1)
                        )
                        .arg(
                            Arg::new("output")
                                .short('o')
                                .num_args(1)
                        )
                )
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
                .arg(Arg::new("string"))
        )
        .subcommand(
            Command::new("stats")
                .about("Show stats about the document index")
                .arg_required_else_help(false)
                .arg(
                    Arg::new("all_terms")
                        .long("all-terms")
                        .num_args(0)
                        .action(ArgAction::SetTrue)
                )
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
            attachments: Vec::new(),
        };

        snip::insert_snip(&conn, &s)?;
        s.index(&conn)?;
        println!("added uuid: {}", s.uuid);
    }

    // ATTACH
    if let Some(("attach", sub_matches)) = matches.subcommand() {

        // ATTACH ADD
        if let Some(("add", attach_sub_matches)) = sub_matches.subcommand() {
            let id_str = attach_sub_matches.get_one::<String>("snip_uuid").ok_or("parsing snip_uuid")?;
            let snip_uuid = snip::search_uuid(&conn, id_str)?;
            // let snip_uuid = Uuid::try_parse(id.as_str())?;

            let files = attach_sub_matches.get_many::<String>("files");
            if let Some(files) = files {
                // construct document (also verifies that the snip_uuid is present)
                let s = snip::get_from_uuid(&conn, &snip_uuid)?;
                println!("{} {}", s.uuid, s.name);

                // add each file
                for f in files {
                    let path = Path::new(f);
                    snip::add_attachment(&conn, snip_uuid, path)?;
                    println!("  added {}", f);
                }
            } else {
                eprintln!("no files specified");
                std::process::exit(1);
            }
        }

        // ATTACH LS
        if let Some(("ls", attach_sub_matches)) = sub_matches.subcommand() {
            let ids = snip::get_attachment_all(&conn)?;
            for id in ids {
                let a = snip::get_attachment_from_uuid(&conn, id)?;

                // uuid
                if attach_sub_matches.get_flag("long") {
                    print!("{} ", a.uuid);
                } else {
                    print!("{} ", snip::split_uuid(&a.uuid)[0]);
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

        // ATTACH RM
        if let Some(("rm", attach_sub_matches)) = sub_matches.subcommand() {
            let ids_args = attach_sub_matches.get_many::<String>("uuids");

            if let Some(ids_str) = &ids_args {
                let total = ids_str.len();
                for (i, id_str) in ids_str.clone().enumerate() {
                    let id = snip::search_attachment_uuid(&conn, id_str)?;
                    let a = snip::get_attachment_from_uuid(&conn, id)?;
                    a.remove(&conn)?;
                    println!("[{}/{}] removed {} {}", i + 1, total, a.uuid, a.name);
                }
            }
        }

        // ATTACH WRITE
        if let Some(("write", attach_sub_matches)) = sub_matches.subcommand() {
            // obtain attachment
            let arg_id = attach_sub_matches.get_one::<String>("id");
            let id_str = match arg_id {
                Some(v) => v,
                None => return Err(Box::new(SnipError::General("no attachment id specified".to_string()))),
            };
            let id = snip::search_attachment_uuid(&conn, id_str)?;
            let a = snip::get_attachment_from_uuid(&conn, id)?;

            // determine output path
            let arg_output = attach_sub_matches.get_one::<String>("output");
            let output: String = match arg_output {
                Some(v) => v.clone(),
                None => a.name.clone(),
            };

            // write file
            a.write(&output)?;
            println!("{} written ({} bytes)", output, a.size);
        }
    }

    // GET
    if let Some(("get", sub_matches)) = matches.subcommand() {
        let id_str = sub_matches
            .get_one::<String>("uuid")
            .ok_or("uuid not present")?;

        // search for unique uuid to allow partial string arg
        let id = snip::search_uuid(&conn, id_str)?;
        let mut s = snip::get_from_uuid(&conn, &id)?;

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

                    // show attachments
                    s.collect_attachments(&conn)?;
                    if !s.attachments.is_empty() {
                        println!("attachments:");

                        println!("{:<36} {:>10} name", "uuid", "bytes");
                        for a in &s.attachments {
                            println!("{} {:>10} {}", a.uuid, a.size, a.name);
                        }
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

    // INDEX
    if let Some(("index", _)) = matches.subcommand() {
        snip::create_index_table(&conn)?;

        let ids = snip::uuid_list(&conn)?;
        let mut status_len: usize;
        eprint!("indexing...");
        for (i, id) in ids.iter().enumerate() {
            let mut s = snip::get_from_uuid(&conn, id)?;

            // display status
            let status = format!("[{}/{}] {}", i + 1, &ids.len(), s.name);
            status_len = status.chars().collect::<Vec<char>>().len();
            eprint!("{}", status);

            // analyze and index document
            s.analyze()?;
            s.index(&conn)?;

            // clear output - rewind, overwrite w/space, rewind
            for _ in 0..status_len {
                eprint!("{} {}", 8u8 as char, 8u8 as char);
            }
        }
        eprintln!("success");
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

            let results = snip::search_all_present(&conn, terms_stem)?;
            for (k, v) in results.items {
                let mut s = snip::get_from_uuid(&conn, &k)?;
                s.analyze()?;
                println!("{}", s.name);
                print!("  {}", snip::split_uuid(&s.uuid)[0]);
                // create summary of terms and counts
                let mut terms_summary: HashMap<String, usize> = HashMap::new();
                for t in &v {
                    for m in t.matches.clone() {
                        terms_summary.insert(m.0, m.1.len());
                    }
                }
                // print!(" {:?} ", terms_summary);
                print!(" [");
                for (i, (k, v)) in terms_summary.iter().enumerate() {
                    print!("{}: {}", k, v);
                    if i != terms_summary.len() - 1 {
                        print!(" ");
                    }
                }
                print!("]");
                println!();

                for item in v {
                    for (term, positions) in item.matches {
                        let position_limit = 5;
                        for (i, pos) in positions.iter().enumerate() {
                            if i != 0 && i == position_limit {
                                eprintln!("    ...additional results: {}", positions.len() - i);
                                break;
                            }
                            // gather context indices and print them
                            let context = s.analysis.get_term_context_positions(*pos, 8);
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

                                if let Some(suffix) = &snip_word.suffix {
                                    if i == context.len() - 1 { // do not print the final suffix
                                        break;
                                    }
                                    // TODO remove repetitive whitespace to conform formatted text to search results
                                    let output_stripped = suffix.replace(['\n', '\r', char::from_u32(0x0au32).unwrap()], " "); // no newlines, etc

                                    let reduce_spaces = |x: String| -> String {
                                        let mut output = String::new();
                                        let mut last_grapheme: &str = "";
                                        for g in x.graphemes(true) {
                                            if g == " " && last_grapheme == " " {
                                                // skip consecutive spaces
                                                continue;
                                            }
                                            output = format!("{}{}", output, g);
                                            last_grapheme = g;
                                        }
                                        output
                                    };

                                    print!("{}", reduce_spaces(output_stripped));
                                }
                            }
                            println!("\"");
                        }
                    }
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

    // SPLIT
    if let Some(("split", sub_matches)) = matches.subcommand() {
        let input = match sub_matches.get_one::<String>("string") {
            Some(v) => v.to_owned(),
            None => snip::read_lines_from_stdin()?,
        };
        let words = input.unicode_words();
        println!("{:?}", words.collect::<Vec<&str>>());
    }

    // STATS
    if let Some(("stats", sub_matches)) = matches.subcommand() {
        let mut max_terms = 20;
        if let Some(all_terms) = sub_matches.get_one::<bool>("all_terms") {
            if *all_terms {
                max_terms = 0;
            }
        }
        let stats = snip::stats_index(&conn)?;
        println!("Terms:");
        println!("  indexed: {}", stats.terms_total);
        println!("  distinct: {}", stats.terms_unique);
        print!("  occurrences:");
        if max_terms != 0 {
            print!(" (top {})", max_terms);
        }
        println!();
        for (i, (term, count)) in stats.terms_with_counts.iter().enumerate() {
            let percentage: f32 = (*count as f32 / stats.terms_total as f32) * 100.0;
            println!("    {:<6} ({:.2}%) {}", count, percentage, term);
            if i >= max_terms && max_terms != 0 {
                break;
            }
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

    Ok(())
}

fn stem_vec(words: Vec<String>) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);
    words.iter().map(|w| stemmer.stem(w).to_string()).collect()
}
