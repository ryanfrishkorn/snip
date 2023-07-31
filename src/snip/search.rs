use std::collections::HashMap;
use std::error::Error;
use rusqlite::Connection;
use uuid::Uuid;
use crate::snip::SnipError;

#[derive(Debug)]
pub struct SearchResult {
    pub items: HashMap<Uuid, Vec<SearchResultItem>>
}

#[derive(Debug)]
pub struct SearchResultItem {
    pub matches: HashMap<String, Vec<usize>>, // <term, Vec<positions>
}

#[derive(Clone, Debug)]
pub struct SearchResultTerm {
    pub uuid: Uuid,
    pub term: String,
    pub positions: Vec<usize>,
}

/// Data structure layout for convenient output with context
///
/// Latin Sample Text
///   c908e14c [lorem: 2, ipsum: 1]
///     [0-8] "Lorem of the sea and the..."
///     [21-29] "Lorem ipsum of the sea and the..."
///
/// This output will require these structs
///
/// Vec<SearchResult>
///
/// struct SearchResult {
///     s: Snip,
///     matches: HashMap<String, Vec<usize>> // (term, Vec<positions>)
///     score: Option<f64>, // this way we can sort by score easily
/// }
///
/// struct IndexResult {
///     term: String,
///     positions: Vec<usize>,
/// }
///
/// The code will be similar to this:
///
/// for result in search_results {
///     println!("{}", result.s.name);
///     println!("  {} {:?}", result.s.uuid,
/// }

/// Returns ids of documents that match the given term
pub fn search_data(conn: &Connection, term: &String) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid FROM snip WHERE data LIKE :term")?;
    let term_fuzzy = format!("{} {}{}", "%", term, "%");

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
    // println!("results: {:?}", results);
    Ok(results)
}

/// Search for term and return a vector containing uuid and vector of term positions
pub fn search_index_term(conn: &Connection, term: &String) -> Result<Vec<(Uuid, Vec<usize>)>, Box<dyn Error>> {
    let mut results: Vec<(Uuid, Vec<usize>)> = Vec::new();
    let mut results_single: (Uuid, Vec<usize>);
    let mut stmt = conn.prepare("SELECT uuid, positions FROM snip_index_rs WHERE term = :term")?;
    let rows = stmt.query_and_then(&[(":term", &term)], |row| -> Result<(String, String), Box<dyn Error>> {
        let id: String = row.get(0)?;
        let positions: String = row.get(1)?;
        Ok((id, positions))
    })?;

    for row in rows.flatten() {
        // parse uuid, split positions string and create vector
        let id: Uuid = Uuid::try_parse(row.0.as_str())?;
        let positions_split: Vec<usize> = row.1.split(',').map(|x| x.parse::<usize>().expect("converting position string to usize")).collect();
        results_single = (id, positions_split);
        results.push(results_single);
        // return Ok(results_single);
    }
    if results.is_empty() {
        return Err(Box::new(SnipError::SearchNoMatches("no matches found in index".to_string())));
    }
    Ok(results)
}

/// Search the index and return uuids that contain term
pub fn search_uuids_matching_term(conn: &Connection, term: String) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut ids: Vec<Uuid> = Vec::new();
    let mut stmt = conn.prepare("SELECT uuid FROM snip_index_rs WHERE term = :term")?;
    let rows = stmt.query_and_then(&[(":term", &term)], |row| -> Result<String, Box<dyn Error>> {
        let id: String = row.get(0)?;
        Ok(id)
    })?;

    for row in rows.flatten() {
        let id = Uuid::try_parse(row.as_str())?;
        ids.push(id);
    }
    Ok(ids)
}

/// Search using a logical combination of terms that must all be present, terms that disqualify if
/// present, and terms that are optional but add to the result score
/*
pub fn search_prototype(conn: &Connection, terms_positive: Vec<String>, _terms_negative: Vec<String>, _terms_optional: Vec<String>) -> Result<Vec<Uuid>, Box<dyn Error>> {
    todo!();
    // let results = search_all_present(conn, terms_positive)?;
    // Ok(results)
}
 */

/// Reduce the input of vectors to a single vector of Uuids that are the intersection of all vectors
///
/// Uuids must be unique in each vector
fn reduce_match_all(input: Vec<Vec<Uuid>>) -> Vec<Uuid> {
    let mut results: Vec<Uuid> = Vec::new();
    let matches_required = input.len();

    for term_results in &input {
        for id in term_results {
            // do not continue if already present in results
            // this also avoids adding duplicates
            if results.contains(id) {
                continue;
            }
            let mut matches = 0;
            for term_results_inner in &input {
                if term_results_inner.contains(id) {
                    matches += 1;
                }
            }
            if matches == matches_required {
                results.push(*id);
            }
        }
    }
    // sort for later comparisons
    results.sort();
    results
}

fn reduce_match_all_terms(input: Vec<Vec<SearchResultTerm>>) -> Vec<SearchResultTerm> {
    let mut results: Vec<SearchResultTerm> = Vec::new();
    let matches_required = input.len();

    for term_results in &input {
        'outer: for search_result in term_results {
            // check if already appended to results
            for r in &results {
                if r.uuid == search_result.uuid {
                    continue 'outer;
                }
            }

            // look for matches
            let mut matches = 0;
            for term_results_inner in &input {
                for r in term_results_inner {
                    if r.uuid == search_result.uuid {
                        matches += 1;
                    }
                }
            }
            if matches == matches_required {
                results.push(search_result.clone())
            }
        }
    }
    results
}

pub fn search_all_present(conn: &Connection, terms: Vec<String>) -> Result<SearchResult, Box<dyn Error>> {
    let mut result = SearchResult {
        items: HashMap::new(),
    };

    let mut result_prelim: Vec<SearchResultTerm> = Vec::new();

    for term in terms {
        let mut stmt = conn.prepare("SELECT uuid, positions FROM snip_index_rs WHERE term = :term")?;
        let query_iter = stmt.query_map(&[
            (":term", &term),
        ], |row| {
            let id = row.get::<_, String>(0)?;
            let pos_str = row.get::<_, String>(1)?;
            Ok((id, pos_str))
        })?;
        for id_str in query_iter.flatten() {
            let uuid = Uuid::try_parse(id_str.0.as_str())?;
            let positions: Vec<usize> = id_str.1.split(',').map(|x| x.parse::<usize>().expect("parsing positions from db string")).collect();
            result_prelim.push(SearchResultTerm{
                uuid,
                term: term.clone(),
                positions,
            });
        }
    }

    // add all matches to result hashmap
    for rt in result_prelim {
        let mut item = SearchResultItem {
            matches: HashMap::new(),
        };
        item.matches.insert(rt.term, rt.positions);

        // add to final results
        if result.items.get(&rt.uuid).is_none() {
            result.items.insert(rt.uuid, Vec::new());
        }
        result.items.get_mut(&rt.uuid).unwrap().push(item); // FIXME - no unwrap
    }
    Ok(result)
}

/// Searches the database index returning UUIDs that match supplied terms
pub fn search_index_terms(conn: &Connection, terms: &Vec<String>) -> Result<HashMap<String, Vec<(Uuid, Vec<usize>)>>, Box<dyn Error>> {
    let mut results: HashMap<String, Vec<(Uuid, Vec<usize>)>> = HashMap::new();

    // search each term
    for term in terms {
        let results_single_term = search_index_term(conn, term)?;
        results.insert(term.clone(), results_single_term);
    }
    Ok(results)
}

/// Search for a uuid matching the supplied partial string.
/// The partial uuid must match a unique record to return the result.
pub fn search_uuid(conn: &Connection, id_partial: &str) -> Result<Uuid, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT uuid from snip WHERE uuid LIKE :id LIMIT 2")?;
    let id_partial_fuzzy = format!("{}{}{}", "%", id_partial, "%");

    let rows = stmt.query_map(&[(":id", &id_partial_fuzzy)], |row| {
        let id_str = row.get(0)?;
        Ok(id_str)
    })?;

    // return only if a singular result is matched, so we check for two results
    let mut id_str = String::new();
    for (i, id) in rows.into_iter().enumerate() {
        if i == 0 {
            id_str = id.unwrap();
        } else {
            return Err(Box::new(SnipError::UuidMultipleMatches(format!("provided partial {} returned multiple document uuids", id_partial))));
        }
    }

    if !id_str.is_empty() {
        return match Uuid::parse_str(&id_str) {
            Ok(v) => Ok(v),
            Err(e) => Err(Box::new(e)),
        };
    }
    Err(Box::new(SnipError::UuidNotFound(format!("document uuid not found using partial {}", id_partial))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::error::Error;
    use uuid::Uuid;
    use crate::snip;
    use crate::snip::test_prep::*;

    #[test]
    fn test_reduce_match_all() -> Result<(), Box<dyn Error>> {
        let a = Uuid::try_parse("00000000-0000-0000-0000-000000000000").unwrap();
        let b = Uuid::try_parse("00000000-0000-0000-0000-000000000001").unwrap();
        let c = Uuid::try_parse("00000000-0000-0000-0000-000000000002").unwrap();
        let d = Uuid::try_parse("00000000-0000-0000-0000-000000000003").unwrap();
        let e = Uuid::try_parse("00000000-0000-0000-0000-000000000004").unwrap();
        let f = Uuid::try_parse("00000000-0000-0000-0000-000000000005").unwrap();

        let input: Vec<Vec<Uuid>> = vec![
            vec![
                a, b, c, d, e, f,
            ],
            vec![
                a, c, d, e, f,
            ],
            vec![
                a, b, c, f,
            ],
        ];

        let expect: Vec<Uuid> = vec![a, c, f];
        let reduced = reduce_match_all(input);
        if expect != reduced {
            panic!("expected {:?} got {:?}", expect, reduced);
        }

        Ok(())
    }

    #[test]
    fn test_reduce_match_all_terms() -> Result<(), Box<dyn Error>> {
        // reduce_match_all_terms(input: Vec<Vec<SearchResultTerm>>) -> Vec<SearchResultTerm> {
        let a = SearchResultTerm {
            uuid: Uuid::try_parse("00000000-0000-0000-0000-000000000000").unwrap(),
            term: "lorem".to_string(),
            positions: vec![0, 1, 2],
        };
        let b = SearchResultTerm {
            uuid: Uuid::try_parse("00000000-0000-0000-0000-000000000001").unwrap(),
            term: "lorem".to_string(),
            positions: vec![0, 1, 2],
        };
        let c = SearchResultTerm {
            uuid: Uuid::try_parse("00000000-0000-0000-0000-000000000002").unwrap(),
            term: "lorem".to_string(),
            positions: vec![0, 1, 2],
        };
        let d = SearchResultTerm {
            uuid: Uuid::try_parse("00000000-0000-0000-0000-000000000003").unwrap(),
            term: "lorem".to_string(),
            positions: vec![0, 1, 2],
        };
        let e = SearchResultTerm {
            uuid: Uuid::try_parse("00000000-0000-0000-0000-000000000004").unwrap(),
            term: "lorem".to_string(),
            positions: vec![0, 1, 2],
        };
        let f = SearchResultTerm {
            uuid: Uuid::try_parse("00000000-0000-0000-0000-000000000005").unwrap(),
            term: "lorem".to_string(),
            positions: vec![0, 1, 2],
        };

        let input: Vec<Vec<SearchResultTerm>> = vec![
            vec![
                a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(),
            ],
            vec![
                a.clone(), c.clone(), d.clone(), e.clone(),
            ],
            vec![
                a.clone(), c.clone(), f.clone(),
            ],
        ];

        let expect: Vec<SearchResultTerm> = vec![a, c];
        let result = reduce_match_all_terms(input);
        // println!("expect: {:#?}", expect);
        println!("result: {:#?}", result);

        for (i, r) in result.iter().enumerate() {
            if r.uuid != expect[i].uuid {
                panic!("failed integrity check");
            }
        }
        Ok(())
    }

    #[test]
    fn test_search_all_present() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        snip::index_all_items(&conn)?;

        let stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);

        let terms: Vec<String> = vec!["lorem".to_string(), "ipsum".to_string(), "dolor".to_string()];
        let stems: Vec<String> = terms.iter().map(|w| stemmer.stem(w).to_string()).collect();
        let result = search_all_present(&conn, stems)?;

        println!("number of results: {}", result.items.len());
        println!("{:#?}", result);
        /*
        for (k, v) in result.items {
            let s = snip::get_from_uuid(&conn, &k)?;
            println!("{} {}", s.uuid, s.name);
            println!("  {:#?}", v);
        }
         */
        Ok(())
    }

    #[test]
    fn test_search_uuid() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");

        let partials: HashMap<String, String> = HashMap::from([
            // ba652e2d-b248-4bcc-b36e-c26c0d0e8002
            (ID_STR[0..8].to_string(), "segment 1".to_string()), // ba652e2d
            (ID_STR[9..13].to_string(), "segment 2".to_string()), // _________b248
            (ID_STR[14..18].to_string(), "segment 3".to_string()), // ______________4bbc
            (ID_STR[19..23].to_string(), "segment 4".to_string()), // ___________________b36e
            (ID_STR[24..].to_string(), "segment 5".to_string()), // ________________________c26c0d0e8002
            (ID_STR[7..12].to_string(), "partial 1".to_string()), // _______d-b24
            (ID_STR[7..14].to_string(), "partial 2".to_string()), // _______d-b248-
            (ID_STR[7..15].to_string(), "partial 3".to_string()), // _______d-b248-4
            (ID_STR[8..19].to_string(), "partial 4".to_string()), // ________-b248-4bcc-
            (ID_STR[23..].to_string(), "partial 5".to_string()), // _______________________-c26c0d0e8002
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