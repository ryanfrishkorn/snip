use crate::snip::SnipError;
use rusqlite::Connection;
use std::collections::HashMap;
use std::error::Error;
use uuid::Uuid;

#[derive(Debug)]
pub struct SearchQuery {
    pub terms_include: Vec<String>, // all terms must be present in a document
    pub terms_exclude: Vec<String>, // none of these terms may be present in a document
    pub terms_optional: Vec<String>, // neither mandatory nor disqualifying, but increase score if present
    pub method: SearchMethod,        // search the index, document text field, etc.
    pub uuids: Vec<Uuid>,
}

#[derive(Debug)]
pub struct SearchQueryResult {
    pub items: Vec<SearchQueryItem>,
}

#[derive(Debug)]
pub struct SearchQueryItem {
    pub uuid: Uuid,
    pub score: Option<f64>,
    pub matches: HashMap<String, Vec<usize>>, // term, positions
}

#[derive(Debug)]
pub enum SearchMethod {
    IndexStem, // index of stemmed terms parsed from document text
    IndexWord, // index of unmodified words parsed from document text
    Literal,   // direct matching on unmodified document text
}

impl SearchQueryResult {
    /*
    /// Score the search results using both the result and the query. This will allow for
    /// scores to be based on the relationship between individual results.
    pub fn score_search_query(query: SearchQuery, result: &mut SearchQueryResult) {
    }
     */
}

/// Search using a logical combination of terms that must all be present, terms that disqualify
/// if present, and terms that are optional but add to the result score
pub fn search_structured(
    conn: &Connection,
    search_query: SearchQuery,
) -> Result<SearchQueryResult, Box<dyn Error>> {
    let mut query_result = SearchQueryResult { items: Vec::new() };
    let mut include_results: Vec<Uuid> = Vec::new();
    let mut exclude_results: Vec<Uuid> = Vec::new();

    // if search uuids are not set, search all documents
    if search_query.uuids.is_empty() {
        // INCLUDE
        for (i, term) in search_query.terms_include.iter().enumerate() {
            let mut result = search_uuids_matching_term(conn, term)?;
            // println!("iter result: {:?}", result);
            // push all results on first run for next iteration comparison
            if i == 0 {
                include_results.append(&mut result);
                // break if there was only one term
                if search_query.terms_include.len() == 1 {
                    break;
                }
                continue;
            }

            // filter non-matching uuids
            include_results.retain_mut(|id| result.contains(id));
        }
        // println!("include_results: {:?}", include_results);

        // EXCLUDE
        for term in search_query.terms_exclude {
            let result = search_uuids_matching_term(conn, &term)?;
            for r in result {
                if !exclude_results.contains(&r) {
                    exclude_results.push(r);
                }
            }
        }
        // println!("exclude_results: {:?}", exclude_results);

        // SUBTRACT EXCLUDE FROM INCLUDE
        include_results.retain_mut(|id| !exclude_results.contains(id));
        // println!("filtered_results: {:?}", include_results);
    } else {
        // restrict search to supplied uuids
        for uuid in search_query.uuids {
            include_results.push(uuid);
        }
    }

    // BUILD OUTPUT
    for uuid in include_results {
        let mut item = SearchQueryItem {
            uuid,
            score: None,
            matches: HashMap::new(),
        };

        // gather and push positions for each term
        for term in search_query.terms_include.iter() {
            let positions = get_term_positions(conn, &uuid, term)?;
            item.matches.insert(term.clone(), positions);
        }
        query_result.items.push(item);
    }

    Ok(query_result)
}

#[derive(Debug)]
pub struct SearchResult {
    pub items: HashMap<Uuid, Vec<SearchTermPositions>>,
}

#[derive(Debug)]
pub struct SearchTermPositions {
    pub matches: HashMap<String, Vec<usize>>, // <term, Vec<positions>
}

#[derive(Clone, Debug)]
pub struct SearchResultTerm {
    pub uuid: Uuid,
    pub term: String,
    pub positions: Vec<usize>,
}

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

fn get_term_positions(
    conn: &Connection,
    id: &Uuid,
    term: &String,
) -> Result<Vec<usize>, Box<dyn Error>> {
    let mut stmt =
        conn.prepare("SELECT positions FROM snip_index_rs WHERE uuid = :uuid AND term = :term")?;
    let query_iter = stmt.query_map(&[(":uuid", &id.to_string()), (":term", term)], |row| {
        let positions = row.get::<_, String>(0)?;
        Ok(positions)
    })?;

    let mut positions: Vec<usize> = Vec::new();
    if let Some(positions_str) = query_iter.flatten().next() {
        positions = positions_str
            .split(',')
            .map(|x| x.parse::<usize>().expect("converting db pos to usize"))
            .collect();
    }
    Ok(positions)
}

/// Search the index and return uuids that contain term
pub fn search_uuids_matching_term(
    conn: &Connection,
    term: &String,
) -> Result<Vec<Uuid>, Box<dyn Error>> {
    let mut ids: Vec<Uuid> = Vec::new();
    let mut stmt = conn.prepare("SELECT uuid FROM snip_index_rs WHERE term = :term")?;
    let rows = stmt.query_and_then(
        &[(":term", &term)],
        |row| -> Result<String, Box<dyn Error>> {
            let id: String = row.get(0)?;
            Ok(id)
        },
    )?;

    for row in rows.flatten() {
        let id = Uuid::try_parse(row.as_str())?;
        ids.push(id);
    }
    Ok(ids)
}

pub fn search_all_present(
    conn: &Connection,
    terms: Vec<String>,
) -> Result<SearchResult, Box<dyn Error>> {
    let mut result = SearchResult {
        items: HashMap::new(),
    };

    let mut result_prelim: Vec<SearchResultTerm> = Vec::new();

    for term in terms {
        let mut stmt =
            conn.prepare("SELECT uuid, positions FROM snip_index_rs WHERE term = :term")?;
        let query_iter = stmt.query_map(&[(":term", &term)], |row| {
            let id = row.get::<_, String>(0)?;
            let pos_str = row.get::<_, String>(1)?;
            Ok((id, pos_str))
        })?;
        for id_str in query_iter.flatten() {
            let uuid = Uuid::try_parse(id_str.0.as_str())?;
            let positions: Vec<usize> = id_str
                .1
                .split(',')
                .map(|x| {
                    x.parse::<usize>()
                        .expect("parsing positions from db string")
                })
                .collect();
            result_prelim.push(SearchResultTerm {
                uuid,
                term: term.clone(),
                positions,
            });
        }
    }

    // add all matches to result hashmap
    for rt in result_prelim {
        let mut item = SearchTermPositions {
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

/// Search for a uuid matching the supplied partial string.
/// The partial uuid must match a unique record to return the result.
pub fn search_uuid(conn: &Connection, id_partial: &str) -> Result<Uuid, SnipError> {
    let mut stmt = match conn.prepare("SELECT uuid from snip WHERE uuid LIKE :id LIMIT 2") {
        Ok(v) => v,
        Err(e) => {
            println!("There was a problem preparing the search query: {}", e);
            return Err(SnipError::General(format!("{}", e)));
        }
    };
    let id_partial_fuzzy = format!("{}{}{}", "%", id_partial, "%");

    let rows = match stmt.query_map(&[(":id", &id_partial_fuzzy)], |row| {
        let id_str = match row.get(0) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        Ok(id_str)
    }) {
        Ok(v) => v,
        Err(e) => return Err(SnipError::General(format!("{}", e))),
    };

    // return only if a singular result is matched, so we check for two results
    let mut id_str = String::new();
    for (i, id) in rows.into_iter().enumerate() {
        if i == 0 {
            id_str = id.unwrap();
        } else {
            return Err(SnipError::UuidMultipleMatches(format!(
                "provided partial {} returned multiple document uuids",
                id_partial
            )));
        }
    }

    if !id_str.is_empty() {
        return match Uuid::parse_str(&id_str) {
            Ok(v) => Ok(v),
            Err(e) => Err(SnipError::General(format!("{}", e))),
        };
    }
    Err(SnipError::UuidNotFound(format!(
        "The document id was not found using id {}",
        id_partial
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snip;
    use crate::snip::test_prep::*;
    use std::collections::HashMap;
    use std::error::Error;
    use uuid::Uuid;

    #[test]
    fn test_search_all_present() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        snip::index_all_items(&conn)?;

        let stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);

        let terms: Vec<String> = vec![
            "lorem".to_string(),
            "ipsum".to_string(),
            "dolor".to_string(),
        ];
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
    fn test_search_structured() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database()?;
        snip::index_all_items(&conn)?;

        let query = SearchQuery {
            // terms_include: vec!["ipsum".to_string(), "dolor".to_string()],
            terms_include: vec!["in".to_string(), "is".to_string()],
            terms_exclude: vec!["fuzz".to_string()],
            terms_optional: vec![],
            method: SearchMethod::IndexStem,
            uuids: vec![],
        };

        let expect = SearchQueryResult {
            items: vec![SearchQueryItem {
                uuid: Uuid::try_parse("412f7ca8-824c-4c70-80f0-4cca6371e45a")?,
                score: None,
                matches: HashMap::from([
                    (
                        "in".to_string(),
                        vec![
                            116, 159, 352, 730, 794, 809, 1043, 1114, 1143, 1317, 1341, 1362, 1397,
                            1417,
                        ],
                    ),
                    (
                        "is".to_string(),
                        vec![
                            100, 110, 359, 591, 715, 806, 818, 938, 954, 1023, 1034, 1053, 1171,
                            1218, 1266, 1370, 1377, 1387, 1393, 1414, 1439, 1512, 1517, 1542, 1591,
                        ],
                    ),
                ]),
            }],
        };

        let result = search_structured(&conn, query)?;
        // println!("expect: {:?}", expect);
        // println!("result: {:?}", result);

        // verify id, length, and keys only
        let expect_item = expect.items.first().expect("getting first expect_item");
        let result_item = result.items.first().expect("getting first result_item");
        if expect_item.uuid != result_item.uuid {
            panic!(
                "expected uuid {} got {}",
                expect_item.uuid, result_item.uuid
            );
        }

        if expect_item.matches != result_item.matches {
            panic!("expected item {:?} got {:?}", expect_item, result_item);
        }

        Ok(())
    }

    #[test]
    fn test_search_structured_uuids() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database()?;
        snip::index_all_items(&conn)?;

        // Lorem ipsum
        let id: Uuid = Uuid::try_parse(ID_STR)?;
        let query = SearchQuery {
            terms_include: vec!["lorem".to_string(), "ipsum".to_string()],
            terms_exclude: vec!["fuzz".to_string()],
            terms_optional: vec![],
            method: SearchMethod::IndexStem,
            uuids: vec![id],
        };
        let result = search_structured(&conn, query)?;
        // println!("result: {:#?}", result);
        let item = result.items.get(0).unwrap();
        // check length of positions for "lorem"
        let item_lorem_len = item.matches.get("lorem").unwrap().len();
        let item_lorem_len_expect = 2;
        if item_lorem_len != item_lorem_len_expect {
            panic!(
                "expected {} matches for 'lorem', got {}",
                item_lorem_len_expect, item_lorem_len
            );
        }
        // check length of positions for "ipsum"
        let item_ipsum_len = item.matches.get("ipsum").unwrap().len();
        let item_ipsum_len_expect = 5;
        if item_ipsum_len != item_ipsum_len_expect {
            panic!(
                "expected {} matches for 'ipsum', got {}",
                item_ipsum_len_expect, item_ipsum_len
            );
        }

        // Fuzzing document
        let id = Uuid::try_parse("990a917e-66d3-404b-9502-e8341964730b")?;
        let query = SearchQuery {
            terms_include: vec!["fuzz".to_string(), "random".to_string()],
            terms_exclude: vec!["lorem".to_string()],
            terms_optional: vec![],
            method: SearchMethod::IndexStem,
            uuids: vec![id],
        };
        let result = search_structured(&conn, query)?;
        // println!("result: {:#?}", result);
        // check length of positions for "fuzz"
        let item = result.items.get(0).unwrap();
        let item_fuzz_len = item.matches.get("fuzz").unwrap().len();
        let item_fuzz_len_expect = 7;
        if item_fuzz_len != item_fuzz_len_expect {
            panic!(
                "expected {} matches for 'fuzz', got {}",
                item_fuzz_len_expect, item_fuzz_len
            );
        }
        // check length of positions for "random"
        let item_random_len = item.matches.get("random").unwrap().len();
        let item_random_len_expect = 1;
        if item_random_len != item_random_len_expect {
            panic!(
                "expected {} matches for 'random', got {}",
                item_random_len_expect, item_random_len
            );
        }

        Ok(())
    }

    #[test]
    fn test_search_uuid() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");

        let id = Uuid::try_parse(ID_STR)?;
        let partials = fragment_uuid(id);

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
