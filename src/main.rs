/*
Snip - a personal information storage and search tool
Copyright (C) 2023, Ryan Frishkorn

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>
*/

use clap::{Arg, ArgAction, Command};
use colored::*;
use rusqlite::{Connection, OpenFlags, Result};
use rust_stemmers::{Algorithm, Stemmer};
use snip::analysis::SnipAnalysis;
use snip::doc::Snip;
use snip::error::SnipError;
use snip::search::{SearchMethod, SearchQuery};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::{IsTerminal, Read};
use std::path::Path;
use unicode_segmentation::UnicodeSegmentation;
use uuid::Uuid;

fn main() -> Result<(), Box<dyn Error>> {
    let version_info = format!(
        "{} ({})\n{}",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH"),
        env!("CARGO_PKG_AUTHORS")
    );
    let cmd = Command::new("snip")
        .bin_name("snip")
        .author(env!("CARGO_PKG_AUTHORS"))
        .long_version(&version_info)
        .arg_required_else_help(true)
        .arg(
            Arg::new("read-only")
                .long("read-only")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("d")
                .short('d')
                .action(ArgAction::Set)
                .help("SQLite database file")
                .value_name("FILE"),
        )
        .subcommand_required(true)
        .subcommand(
            Command::new("add")
                .about("Add new snip to database")
                .arg_required_else_help(false)
                .arg(
                    Arg::new("file")
                        .help("document text from file")
                        .short('f')
                        .long("file")
                        .num_args(1),
                )
                .arg(
                    Arg::new("name")
                        .help("name of new document")
                        .short('n')
                        .long("name")
                        .num_args(1)
                        .action(ArgAction::Set),
                )
                .arg(
                    Arg::new("verbose")
                        .help("verbose (pass output to stdout)")
                        .short('v')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("completion")
                .arg_required_else_help(true)
                .arg(Arg::new("shell").help("shell name")),
        )
        .subcommand(
            Command::new("rename")
                .about("Rename document")
                .arg_required_else_help(true)
                .arg(Arg::new("uuid").help("partial/full id of document"))
                .arg(Arg::new("name").help("new name")),
        )
        .subcommand(
            Command::new("attach")
                .about("Attach binary data to document")
                .subcommand_required(true)
                .subcommand(
                    Command::new("add")
                        .about("add file to document")
                        .arg_required_else_help(true)
                        .arg(Arg::new("snip_uuid").num_args(1))
                        .arg(Arg::new("files").action(ArgAction::Append)),
                )
                .subcommand(
                    Command::new("ls")
                        .about("list attachments")
                        .arg_required_else_help(false)
                        .arg(
                            Arg::new("doc")
                                .help("display associated document uuid")
                                .short('d')
                                .long("doc")
                                .num_args(0)
                                .action(ArgAction::SetTrue),
                        )
                        .arg(
                            Arg::new("long")
                                .help("display full uuid")
                                .short('l')
                                .num_args(0)
                                .action(ArgAction::SetTrue),
                        )
                        .arg(
                            Arg::new("number")
                                .help("number of attachments to list")
                                .short('n')
                                .num_args(1)
                                .action(ArgAction::Append),
                        )
                        .arg(
                            Arg::new("size")
                                .help("display size in bytes")
                                .short('s')
                                .num_args(0)
                                .action(ArgAction::SetTrue),
                        )
                        .arg(
                            Arg::new("time")
                                .help("display timestamp")
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
                                .help("partial/full uuids of documents to remove")
                                .action(ArgAction::Append),
                        ),
                )
                .subcommand(
                    Command::new("write")
                        .about("write attachment to local file")
                        .arg_required_else_help(true)
                        .arg(Arg::new("id").num_args(1))
                        .arg(Arg::new("output").short('o').num_args(1)),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get from uuid")
                .arg_required_else_help(true)
                .arg(Arg::new("uuid"))
                .arg(
                    Arg::new("analyze")
                        .help("print analyzed document text")
                        .long("analyze")
                        .short('a')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("raw")
                        .help("print raw document text only (no headers)")
                        .long("raw")
                        .short('r')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("import").arg_required_else_help(true).arg(
                Arg::new("files")
                    .help("import document files")
                    .required(true)
                    .action(ArgAction::Append),
            ),
        )
        .subcommand(
            Command::new("index")
                .about("Reindex the database")
                .arg_required_else_help(false),
        )
        .subcommand(
            Command::new("ls")
                .about("List snips")
                .arg(
                    Arg::new("all")
                        .help("all documents")
                        .short('a')
                        .long("all")
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("long")
                        .help("display full uuid")
                        .short('l')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("number")
                        .help("number of documents to list")
                        .short('n')
                        .num_args(1)
                        .default_value("20")
                        .action(ArgAction::Append),
                )
                .arg(
                    Arg::new("size")
                        .help("display size in bytes")
                        .short('s')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("time")
                        .help("display timestamp")
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
                .about("Search for terms within all documents")
                .arg_required_else_help(true)
                .arg(
                    Arg::new("exclude")
                        .help("exclude these comma delineated terms")
                        .short('x')
                        .long("exclude")
                        .value_delimiter(',')
                        .action(ArgAction::Append),
                )
                .arg(
                    Arg::new("count")
                        .help("print match count only (no excerpts)")
                        .short('c')
                        .long("count")
                        .num_args(0)
                        .required(false)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("context")
                        .help("number of surrounding context words displayed")
                        .short('C')
                        .long("context")
                        .num_args(1)
                        .required(false)
                        .action(ArgAction::Append),
                )
                .arg(
                    Arg::new("excerpt-limit")
                        .help("limit the number of match excerpts displayed")
                        .long("excerpts")
                        .num_args(1)
                        .action(ArgAction::Append),
                )
                .arg(
                    Arg::new("limit")
                        .help("max number of documents to match")
                        .short('l')
                        .long("limit")
                        .num_args(1)
                        .action(ArgAction::Append),
                )
                .arg(
                    Arg::new("raw")
                        .help("do not strip newlines or returns from search excerpt")
                        .long("raw")
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("uuid")
                        .help("search for matches in specified documents only")
                        .short('u')
                        .long("uuid")
                        .action(ArgAction::Append)
                        .required(false)
                        .value_delimiter(','),
                )
                .arg(Arg::new("terms").action(ArgAction::Append).required(true)),
        )
        .subcommand(
            Command::new("split")
                .about("Split stdin into words")
                .arg_required_else_help(false)
                .arg(Arg::new("string")),
        )
        .subcommand(
            Command::new("stats")
                .about("Show stats about the document index")
                .arg_required_else_help(false)
                .arg(
                    Arg::new("all_terms")
                        .long("all-terms")
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("stem")
                .about("Stem word from stdin")
                .arg_required_else_help(false)
                .arg(Arg::new("words")),
        )
        .subcommand(
            Command::new("update")
                .about("Update document from modified file")
                .arg_required_else_help(true)
                .arg(
                    Arg::new("file")
                        .help("edited document file")
                        .required(true)
                        .num_args(1)
                        .action(ArgAction::Append),
                )
                .arg(
                    Arg::new("remove")
                        .help("remove document file on successful update")
                        .short('r')
                        .num_args(0)
                        .action(ArgAction::SetTrue),
                ),
        );

    // completion script embeds
    let completion_bash = include_bytes!("../completions/bash");
    let matches = cmd.get_matches();
    let db_file_default = ".snip.sqlite3".to_string();
    let home_dir = match env::var("HOME") {
        Ok(v) => v,
        Err(e) => panic!("Could not obtain HOME env: {}", e),
    };
    let db_path: String = match matches.get_one::<String>("d") {
        Some(v) => v.to_owned(),
        None => env::var("SNIP_DB").unwrap_or(format!("{}/{}", home_dir, db_file_default)),
    };

    let conn = match matches.get_flag("read-only") {
        true => Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?,
        false => Connection::open(db_path)?,
    };
    // ensure that tables are present for basic functionality
    snip::doc::create_snip_tables(&conn)?;

    // process all subcommands as in: https://docs.rs/clap/latest/clap/_derive/_cookbook/git/index.html
    // ADD
    if let Some(("add", sub_matches)) = matches.subcommand() {
        // document text
        let mut text: String = String::new();
        match sub_matches.get_one::<String>("file") {
            Some(v) => text = std::fs::read_to_string(v)?,
            None => {
                std::io::stdin().read_to_string(&mut text)?; // FIXME I don't like this
            }
        };

        // name from arg or generate from text
        let name: String = match sub_matches.get_one::<String>("name") {
            Some(v) => v.clone(),
            None => snip::doc::generate_name(&text, 6)?,
        };

        // create document
        let mut s = Snip {
            uuid: Uuid::new_v4(),
            name: name.to_owned(),
            timestamp: chrono::Utc::now(),
            text,
            analysis: SnipAnalysis { words: vec![] },
            attachments: Vec::new(),
        };

        s.insert(&conn)?;
        s.index(&conn)?;
        if sub_matches.get_flag("verbose") {
            print!("{}", s.text);
        }
        println!("added uuid: {}", s.uuid);
    }

    // ATTACH
    if let Some(("attach", sub_matches)) = matches.subcommand() {
        // ATTACH ADD
        if let Some(("add", attach_sub_matches)) = sub_matches.subcommand() {
            let id_str = attach_sub_matches
                .get_one::<String>("snip_uuid")
                .ok_or("parsing snip_uuid")?;
            let snip_uuid = snip::search::search_uuid(&conn, id_str)?;
            // let snip_uuid = Uuid::try_parse(id.as_str())?;

            let files = attach_sub_matches.get_many::<String>("files");
            if let Some(files) = files {
                // construct document (also verifies that the snip_uuid is present)
                let s = snip::doc::get_from_uuid(&conn, &snip_uuid)?;
                println!("{} {}", s.uuid, s.name);

                // add each file
                for f in files {
                    let path = Path::new(f);
                    snip::attachment::add_attachment(&conn, snip_uuid, path)?;
                    println!("  added {}", f);
                }
            } else {
                eprintln!("no files specified");
                std::process::exit(1);
            }
        }

        // ATTACH LS
        if let Some(("ls", attach_sub_matches)) = sub_matches.subcommand() {
            // let ids = snip::get_attachment_all(&conn)?;
            // let full_uuid = attach_sub_matches.get_flag("long");
            // let show_time = attach_sub_matches.get_flag("time");

            // let heading = create_heading(full_uuid, show_time);
            // println!("{}", heading.bright_black());

            let mut header = ListHeading {
                kind: ListHeadingKind::Attachment,
                columns: Vec::new(),
            };

            // uuid (mandatory)
            if attach_sub_matches.get_flag("long") {
                header.add("uuid", 36, ListHeadingAlignment::Left);
            } else {
                header.add("uuid", 8, ListHeadingAlignment::Left);
            }

            // associated document uuid
            if attach_sub_matches.get_flag("doc") {
                header.add("document", 8, ListHeadingAlignment::Left)
            }

            // time
            if attach_sub_matches.get_flag("time") {
                header.add("time", 33, ListHeadingAlignment::Left);
            }

            // size
            if attach_sub_matches.get_flag("size") {
                header.add("size", 9, ListHeadingAlignment::Right);
            }

            // name (mandatory)
            header.add("name", 0, ListHeadingAlignment::Left);

            let mut limit: Option<usize> = None;
            if let Some(v) = attach_sub_matches.get_one::<String>("number") {
                limit = Some(v.parse::<usize>()?);
            }

            // print listing
            if std::io::stdout().is_terminal() {
                eprintln!("{}", header.build().bright_black());
            }
            list_attachments(&conn, header, limit)?;
        }

        // ATTACH RM
        if let Some(("rm", attach_sub_matches)) = sub_matches.subcommand() {
            let ids_args = attach_sub_matches.get_many::<String>("uuids");

            if let Some(ids_str) = &ids_args {
                let total = ids_str.len();
                for (i, id_str) in ids_str.clone().enumerate() {
                    let id = snip::attachment::search_attachment_uuid(&conn, id_str)?;
                    let a = snip::attachment::get_attachment_from_uuid(&conn, id)?;
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
                None => {
                    return Err(Box::new(SnipError::General(
                        "no attachment id specified".to_string(),
                    )))
                }
            };
            let id = snip::attachment::search_attachment_uuid(&conn, id_str)?;
            let a = snip::attachment::get_attachment_from_uuid(&conn, id)?;

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

    // COMPLETION
    if let Some(("completion", sub_matches)) = matches.subcommand() {
        let shell_name = sub_matches
            .get_one::<String>("shell")
            .ok_or("shell name not provided")?;
        if shell_name == "bash" {
            println!("{}", std::str::from_utf8(completion_bash)?);
        }
    }

    // GET
    if let Some(("get", sub_matches)) = matches.subcommand() {
        let id_str = sub_matches
            .get_one::<String>("uuid")
            .ok_or("uuid not present")?;

        // search for unique uuid to allow partial string arg
        let id = match snip::search::search_uuid(&conn, id_str) {
            Ok(v) => v,
            Err(e) => {
                match &e {
                    SnipError::UuidNotFound(s) => println!("{}", s),
                    SnipError::UuidMultipleMatches(s) => println!("{}", s),
                    _ => return Err(Box::new(e)),
                }
                return Ok(());
            }
        };
        let mut s = snip::doc::get_from_uuid(&conn, &id)?;

        // check for raw or formatted output
        if let Some(raw) = sub_matches.get_one::<bool>("raw") {
            match raw {
                // raw output
                true => print!("{}", s.text),
                // formatted output
                false => {
                    s.collect_attachments(&conn)?;
                    s.print();
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

    // IMPORT
    if let Some(("import", sub_matches)) = matches.subcommand() {
        if let Some(files) = sub_matches.get_many::<String>("files") {
            let mut errors = false;
            // Note: attachments are ignored and not imported
            for file in files.into_iter() {
                print!("importing {:?}...", file);

                let mut s = snip::doc::from_file(file)?;
                if snip::doc::get_from_uuid(&conn, &s.uuid).is_ok() {
                    println!("refusing duplicate insert {}", s.uuid);
                    errors = true;
                    // proceed but notify of errors after iterations
                    continue;
                }

                // check for existing id to avoid duplicates
                s.insert(&conn)?;
                // always index after import
                s.index(&conn)?;
                println!("success");
            }
            if errors {
                eprintln!("WARNING: Some documents were not imported.");
            }
        }
    }

    // INDEX
    if let Some(("index", _)) = matches.subcommand() {
        // creation is conditional on non-existence
        snip::doc::create_index_table(&conn)?;
        // clear all data to ensure consistency
        snip::doc::clear_index(&conn)?;

        let ids = snip::doc::uuid_list(&conn, None)?;
        let mut status_len: usize;
        eprint!("indexing...");
        for (i, id) in ids.iter().enumerate() {
            let mut s = snip::doc::get_from_uuid(&conn, id)?;

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
        let mut header = ListHeading {
            kind: ListHeadingKind::Document,
            columns: Vec::new(),
        };

        if let Some(arg_matches) = matches.subcommand_matches("ls") {
            // uuid is required for now, so push either way
            if arg_matches.get_flag("long") {
                header.add("uuid", 36, ListHeadingAlignment::Left);
            } else {
                header.add("uuid", 8, ListHeadingAlignment::Left);
            }

            // time
            header.add("time", 33, ListHeadingAlignment::Left);

            // size
            if arg_matches.get_flag("size") {
                header.add("size", 9, ListHeadingAlignment::Right);
            }

            // name
            header.add("name", 0, ListHeadingAlignment::Left);

            // check for limit
            let mut limit: Option<usize> = None;
            if let Some(v) = arg_matches.get_one::<String>("number") {
                limit = Some(v.parse::<usize>()?);
            }

            // check for all flag
            if arg_matches.get_flag("all") {
                limit = None;
            }

            if std::io::stdout().is_terminal() {
                eprintln!("{}", header.build().bright_black());
            }
            list_items(&conn, header, limit)?;
        }
    }

    // RENAME
    if let Some(("rename", sub_matches)) = matches.subcommand() {
        let id_str = match sub_matches.get_one::<String>("uuid") {
            Some(v) => v.to_string(),
            None => return Err(Box::new(SnipError::General("missing uuid".to_string()))),
        };
        let id = snip::search::search_uuid(&conn, id_str.as_str())?;

        // new name
        let name = match sub_matches.get_one::<String>("name") {
            Some(v) => v.to_string(),
            None => return Err(Box::new(SnipError::General("missing name".to_string()))),
        };

        let mut s = snip::doc::get_from_uuid(&conn, &id)?;
        s.name = name;

        // write changes
        s.update(&conn)?;
    }

    // RM
    if let Some(("rm", sub_matches)) = matches.subcommand() {
        if let Some(args) = sub_matches.get_many::<String>("ids") {
            // convert to uuid
            let ids_str: Vec<String> = args.map(|x| x.to_string()).collect();
            for (i, id_str) in ids_str.iter().enumerate() {
                // obtain full id
                let id = snip::search::search_uuid(&conn, id_str)?;
                let s = snip::doc::get_from_uuid(&conn, &id)?;
                snip::doc::remove_snip(&conn, id)?;
                println!("{}/{} removed {} {}", i + 1, ids_str.len(), id, s.name);
            }
        }
    }

    // SEARCH
    if let Some(("search", sub_matches)) = matches.subcommand() {
        if let Some(args) = sub_matches.get_many::<String>("terms") {
            let terms: Vec<String> = args.map(|x| x.to_owned()).collect();
            let terms_stem = stem_vec(terms.clone());
            let mut terms_exclude: Vec<String> = Vec::new();
            let mut context_raw = false;

            // filter out duplicate search terms if present
            let mut seen_terms: Vec<String> = Vec::new();
            let terms_include: Vec<String> = terms_stem
                .into_iter()
                .filter(|x| {
                    if seen_terms.contains(x) {
                        return false;
                    }
                    seen_terms.push(x.clone());
                    true
                })
                .collect();

            // restrict to specific uuids if supplied
            let mut uuids: Vec<Uuid> = Vec::new();
            if let Some(all_ids_str) = sub_matches.get_many::<String>("uuid") {
                for id_str in all_ids_str {
                    let id = snip::search::search_uuid(&conn, id_str)?;
                    uuids.push(id);
                }
            }

            // exclusionary terms
            if let Some(args) = sub_matches.get_many::<String>("exclude") {
                terms_exclude = stem_vec(args.map(|x| x.to_owned()).collect());
            }

            // establish document limit
            let mut limit: Option<usize> = None;
            if let Some(document_limit) = sub_matches.get_one::<String>("limit") {
                limit = Some(document_limit.parse::<usize>()?);
            }

            // all excerpts are printed by default, unless a maximum per document is set
            let mut excerpts: Option<usize> = None;
            if let Some(limit) = sub_matches.get_one::<String>("excerpt-limit") {
                excerpts = Some(limit.parse::<usize>()?);
            }

            // establish number of surrounding context words to display
            let mut context_words = 6;
            if let Some(context) = sub_matches.get_one::<String>("context") {
                context_words = context.parse::<usize>()?;
            }

            // check if raw search context is desired
            if let Some(raw) = sub_matches.get_one::<bool>("raw") {
                context_raw = *raw;
            }

            // perform search and print summary
            let search_query = SearchQuery {
                terms_include: terms_include.clone(),
                terms_exclude: terms_exclude.clone(),
                terms_optional: vec![],
                method: SearchMethod::IndexStem,
                uuids,
                limit,
            };
            let search_results = match snip::search::search_structured(&conn, search_query) {
                Ok(v) => v,
                Err(e) => return Err(Box::new(e)),
            };

            // exit if no results are present
            if search_results.items.is_empty() {
                return Ok(());
            }

            // print to stderr to keep redirection clean
            eprint!("document");
            if search_results.items.len() != 1 {
                eprint!("s");
            }
            eprint!(": {}", search_results.items.len());
            // print count of matched terms
            let mut term_match_count = 0;
            for item in &search_results.items {
                for m in &item.matches {
                    term_match_count += m.1.len();
                }
            }
            eprintln!(" occurrences: {}", term_match_count);

            // we don't need excerpts for count only
            if !sub_matches.get_flag("count") {
                for item in &search_results.items {
                    let mut s = snip::doc::get_from_uuid(&conn, &item.uuid)?;
                    s.analyze()?;
                    println!("{}", s.name.white());
                    print!("  {}", snip::doc::split_uuid(&s.uuid)[0].bright_blue());

                    // create and print a summary of terms and counts
                    let mut terms_summary: HashMap<String, usize> = HashMap::new();
                    for (term, positions) in &item.matches {
                        terms_summary.insert(term.clone(), positions.len());
                    }
                    print!(" [");
                    // use argument terms vector to order by term
                    for (i, term) in terms_include.iter().enumerate() {
                        if let Some(count) = terms_summary.get(term.as_str()) {
                            print!("{}: {}", term, count);
                            if i != terms_summary.len() - 1 {
                                print!(" ");
                            }
                        }
                    }
                    print!("]");
                    println!();

                    // for each position, gather context and display
                    for term in &terms_include {
                        if let Some(positions) = item.matches.get(term.as_str()) {
                            for (i, pos) in positions.iter().enumerate() {
                                // if limit is hit, break immediately
                                if let Some(e_limit) = excerpts {
                                    if i == e_limit {
                                        break;
                                    }
                                }

                                // this gathers an excerpt from the supplied position
                                let excerpt =
                                    s.analysis.get_excerpt(pos, context_words, context_raw)?;
                                excerpt.print(context_raw);
                            }
                        }
                    }
                    println!();
                }
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
            None => snip::doc::read_lines_from_stdin()?,
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
        let stats = snip::analysis::stats_index(&conn)?;
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
            None => snip::doc::read_lines_from_stdin()?,
        };
        let words = input.unicode_words().collect::<Vec<&str>>();
        let stemmer = Stemmer::create(Algorithm::English);
        let mut stems: Vec<String> = Vec::new();
        for w in words.iter() {
            stems.push(stemmer.stem(w.to_lowercase().as_str()).to_string());
        }
        println!("{:?}", stems);
    }

    // UPDATE
    if let Some(("update", sub_matches)) = matches.subcommand() {
        if let Some(file) = sub_matches.get_one::<String>("file") {
            let s = snip::doc::from_file(file)?;
            s.update(&conn)?;
            let mut s = snip::doc::get_from_uuid(&conn, &s.uuid)?;
            // re-index due to changed content
            s.index(&conn)?;
            eprintln!("update successful");

            // collect attachments before printing so they are included in output
            s.collect_attachments(&conn)?;
            s.print();

            // remove modified document file if requested
            if sub_matches.get_flag("remove") {
                match std::fs::remove_file(file) {
                    Ok(_) => eprintln!("removed {}", file),
                    Err(e) => eprintln!("error removing file {}: {}", file, e),
                }
            }
        } else {
            eprintln!("update failed");
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Data structure for headings of lists
struct ListHeading {
    kind: ListHeadingKind,
    columns: Vec<ListHeadingPosition>,
}

impl ListHeading {
    pub fn add(&mut self, name: &str, width: usize, align: ListHeadingAlignment) {
        let name = name.to_string();

        // create and append position to self
        let position = ListHeadingPosition { name, width, align };

        self.columns.push(position);
    }
}

enum ListHeadingKind {
    Document,
    Attachment,
}

/// Represents the prefix and suffix of a column header
struct ListHeadingPosition {
    name: String,
    width: usize,
    align: ListHeadingAlignment,
}

enum ListHeadingAlignment {
    Left,
    Right,
}

impl ListHeading {
    /// Builds a string to display field headers for listings
    pub fn build(&self) -> String {
        let mut output = String::new();

        // iterate over all fields to establish column width
        for column in self.columns.iter() {
            // println!("column: {:?}", column);
            let mut prefix: String = String::new();
            let mut suffix: String = String::new();
            match column.align {
                ListHeadingAlignment::Left => {
                    if column.width >= column.name.len() {
                        for _ in 0..(column.width - column.name.len()) {
                            suffix.push(' ');
                        }
                    }
                }
                ListHeadingAlignment::Right => {
                    if column.width >= column.name.len() {
                        for _ in 0..(column.width - column.name.len()) {
                            prefix.push(' ');
                        }
                    }
                }
            }

            output = format!("{}{}{}{} ", output, prefix, column.name, suffix);
        }
        output
    }
}

fn list_items(
    conn: &Connection,
    heading: ListHeading,
    limit: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let ids = snip::doc::uuid_list(conn, limit)?;

    for id in ids {
        // establish required data
        let uuid: Uuid;
        let time: String;
        let size: String;
        let name: String;

        match heading.kind {
            ListHeadingKind::Document => {
                let document = snip::doc::get_from_uuid(conn, &id)?;

                uuid = document.uuid;
                time = document.timestamp.to_utc().to_string();
                size = document.text.len().to_string();
                name = document.name.clone();
            }
            ListHeadingKind::Attachment => {
                let attachment = snip::attachment::get_attachment_from_uuid(conn, id)?;

                uuid = attachment.uuid;
                time = attachment.timestamp.to_string();
                size = attachment.size.to_string();
                name = attachment.name.clone();
            }
        };

        // check if specified
        for col in &heading.columns {
            let str = match col.name.as_str() {
                "uuid" => uuid.to_string().bright_blue(),
                "time" => time.bright_black(),
                "size" => size.white(),
                "name" => name.clone().white(),
                _ => {
                    return Err(Box::new(SnipError::General(
                        "invalid column name supplied".to_string(),
                    )))
                }
            };
            // eprintln!("prefix: {} suffix: {}", col.prefix, col.suffix);
            match col.name.as_str() {
                "uuid" => match col.width {
                    v if v <= 8 => print!("{} ", snip::doc::split_uuid(&uuid)[0].bright_blue()),
                    _ => print!("{} ", uuid.to_string().bright_blue()),
                },
                "size" => print!("{:>9} ", str),
                _ => print!("{} ", str),
            }
        }
        println!();
    }

    Ok(())
}

fn list_attachments(
    conn: &Connection,
    heading: ListHeading,
    limit: Option<usize>,
) -> Result<(), Box<dyn Error>> {
    let ids = snip::attachment::get_attachment_all(conn)?;

    for (i, id) in ids.into_iter().enumerate() {
        if let Some(v) = limit {
            if i == v {
                break;
            }
        }

        let attachment = snip::attachment::get_attachment_from_uuid(conn, id)?;
        let uuid = attachment.uuid;
        let uuid_document = attachment.snip_uuid;
        let time = attachment.timestamp.to_string();
        let size = attachment.size.to_string();
        let name = attachment.name.clone();

        for col in &heading.columns {
            let str = match col.name.as_str() {
                "uuid" => uuid.to_string().bright_blue(),
                "document" => snip::doc::split_uuid(&uuid_document)[0]
                    .to_string()
                    .bright_blue(),
                "time" => time.bright_black(),
                "size" => size.white(),
                "name" => name.clone().white(),
                _ => {
                    return Err(Box::new(SnipError::General(
                        "invalid column name supplied".to_string(),
                    )))
                }
            };
            // eprintln!("prefix: {} suffix: {}", col.prefix, col.suffix);
            match col.name.as_str() {
                "uuid" => match col.width {
                    v if v <= 8 => print!("{} ", snip::doc::split_uuid(&uuid)[0].bright_blue()),
                    _ => print!("{} ", uuid.to_string().bright_blue()),
                },
                "size" => print!("{:>9} ", str),
                _ => print!("{} ", str),
            }
        }
        println!();
    }

    Ok(())
}

fn stem_vec(words: Vec<String>) -> Vec<String> {
    let stemmer = Stemmer::create(Algorithm::English);
    words
        .iter()
        .map(|w| w.to_lowercase())
        .map(|w| stemmer.stem(w.as_str()).to_string())
        .collect()
}
