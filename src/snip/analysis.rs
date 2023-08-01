use colored::*;
use std::error::Error;
use rusqlite::Connection;
use unicode_segmentation::UnicodeSegmentation;

/// Analysis of the document derived from
#[derive(Debug)]
pub struct SnipAnalysis {
    pub words: Vec<SnipWord>,
}

#[derive(Debug)]
pub struct AnalysisStats {
    pub terms_total: u64,
    pub terms_unique: u64,
    pub terms_with_counts: Vec<(String, u64)>,
}

#[derive(Debug)]
pub struct Excerpt {
    pub position_first: usize,
    pub position_last: usize,
    pub positions: Vec<usize>,
    pub terms: Vec<ExcerptTerm>,
}

impl Excerpt {
    /// Prints the formatted document excerpt
    pub fn print(&self) {
        print!("    [{}-{}] ", self.position_first, self.position_last);
        print!("\"");
        for (i, term) in self.terms.iter().enumerate() {
            // highlight if appropriate
            match term.highlight {
                true => print!("{}", term.term.red()),
                false => print!("{}", term.term),
            };

            // trim end whitespace on the final suffix, to look clean and preserve punctuation
            if i == self.terms.len() - 1 {
                print!("{}", term.suffix_clean.trim_end());
            } else {
                print!("{}", term.suffix_clean);
            }
        }
        print!("\"");
        println!();
    }
}

#[derive(Debug)]
pub struct ExcerptTerm {
    pub stem: String,
    pub term: String,
    pub highlight: bool,
    pub range_prefix: (usize, usize),
    pub suffix_clean: String,
}

/// Represents a word in the document, along with meta information derived from document analysis
#[derive(Debug)]
pub struct SnipWord {
    pub word: String,
    pub stem: String,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
    pub index: Option<WordIndex>,
}

impl SnipAnalysis {
    pub fn get_excerpt(&self, pos: &usize) -> Result<Excerpt, Box<dyn Error>> {
        let term = &self.words[*pos].stem;
        let mut excerpt = Excerpt {
            position_first: 0,
            position_last: 0,
            positions: Vec::new(),
            terms: Vec::new(),
        };

        // gather context indices and print them
        let positions = self.get_term_context_positions(*pos, 8);
        let position_first = positions.first().ok_or("finding first context position")?;
        let position_last = positions.last().ok_or("finding last context position")?;
        excerpt.position_first = *position_first;
        excerpt.position_last = *position_last;

        for (i, p) in positions.iter().enumerate() {
            excerpt.positions.push(*p);
            let snip_word = &self.words[*p];
            let mut excerpt_term = ExcerptTerm {
                stem: snip_word.stem.clone(),
                term: snip_word.word.clone(),
                highlight: false,
                range_prefix: (*position_first, *position_last),
                suffix_clean: String::new(),
            };
            // check for matching word
            if snip_word.stem == *term {
                excerpt_term.highlight = true;
            }

            if let Some(suffix) = &snip_word.suffix {
                if i == positions.len() - 1 { // do not print the final suffix
                    // break;
                }
                let suffix_stripped = suffix.replace(['\n', '\r', char::from_u32(0x0au32).unwrap()], " "); // no newlines, etc
                // remove repetitive whitespace to conform formatted text to search results
                excerpt_term.suffix_clean = collapse_spaces(suffix_stripped);
            }

            excerpt.terms.push(excerpt_term);
        }
        Ok(excerpt)
    }

    /// get vector positions of desired context including term position
    fn get_term_context_positions(&self, position: usize, count: usize) -> Vec<usize> {
        let mut context: Vec<usize> = Vec::new();
        let mut context_prefix: Vec<usize> = Vec::new();
        let mut context_suffix: Vec<usize> = Vec::new();
        // println!("term: \"{}\" position: {}", &self.words[position].word, position);

        // check bounds of context start
        let context_prefix_pos: usize = match position as i64 - count as i64 {
            x if x <= 0 => 0, // use position zero
            x => x as usize,
        };
        // println!("context_prefix_pos: {}", context_prefix_pos);

        // check bounds of context stop
        let context_suffix_pos: usize = match position + 1 {
            x if x > self.words.len() => self.words.len(),
            x => x,
        };
        // println!("context_suffix_pos: {}", context_suffix_pos);

        for (i, _) in self.words.iter().enumerate() {
            if i >= context_prefix_pos && i < position {
                context_prefix.push(i);
            }
            if i > position && i < context_suffix_pos + count {
                context_suffix.push(i);
            }
        }
        // println!("prefix: {:?}", context_prefix);
        context.append(&mut context_prefix);
        context.push(position);
        // println!("suffix: {:?}", context_suffix);
        context.append(&mut context_suffix);
        context
    }

    /// get document words corresponding to the given context positions
    pub fn get_term_context_words(&self, context: Vec<usize>) -> Vec<&SnipWord> {
        let mut words: Vec<String> = Vec::new();
        let mut snip_words: Vec<&SnipWord> = Vec::new();

        for pos in context {
            words.push(self.words[pos].word.clone());
            snip_words.push(&self.words[pos]);
        }
        snip_words
    }
}

#[derive(Debug)]
pub struct WordIndex {
    pub count: u64,
    pub positions: Vec<u64>,
    pub term: String,
}

impl WordIndex {
    pub fn positions_to_string(&self) -> String {
        let joined: Vec<String> = self.positions.iter().map(|x| x.to_string()).collect();
        joined.join(",")
    }

    pub fn positions_to_u64(pos: String) -> Result<Vec<u64>, Box<dyn Error>> {
        let split: Vec<String> = pos.split(',').map(|x| x.to_string()).collect();
        let mut output: Vec<u64> = Vec::new();
        for n in split {
            let n_u64: u64 = n.parse::<u64>()?;
            output.push(n_u64);
        }
        Ok(output)
    }
}

/// Collapse recurring space characters in a string
fn collapse_spaces(s: String) -> String {
    let mut output = String::new();
    let mut last_grapheme: &str = "";
    for g in s.graphemes(true) {
        if g == " " && last_grapheme == " " {
            // skip consecutive spaces
            continue;
        }
        output = format!("{}{}", output, g);
        last_grapheme = g;
    }
    output
}

/// provide stats about the document and index
pub fn stats_index(conn: &Connection) -> Result<AnalysisStats, Box<dyn Error>> {
    let mut stats = AnalysisStats {
        terms_with_counts: Vec::new(),
        terms_total: 0,
        terms_unique: 0,
    };

    // gather terms information
    let mut stmt = conn.prepare("SELECT SUM(count) FROM snip_index_rs")?;
    let row = stmt.query_and_then([], |row| -> Result<usize, rusqlite::Error>{
        let total = row.get(0)?;
        Ok(total)
    })?;

    if let Some(total) = row.flatten().next() {
        stats.terms_total = total as u64;
    }

    // terms and their popularity across all
    let mut stmt = conn.prepare("SELECT term, SUM(count) FROM snip_index_rs GROUP BY term ORDER BY SUM(count) DESC")?;
    let query_iter = stmt.query_and_then([], |row| -> Result<(String, u64), Box<dyn Error>> {
        let term: String = row.get(0)?;
        let count: usize = row.get(1)?;
        Ok((term, count as u64))
    })?;

    for row in query_iter.flatten() {
        stats.terms_with_counts.push(row);
    }

    // unique terms from index
    let mut stmt = conn.prepare("SELECT count(DISTINCT(term)) FROM snip_index_rs")?;
    let query_iter = stmt.query_and_then([], |row| -> Result<usize, Box<dyn Error>> {
        let total: usize = row.get(0)?;
        Ok(total)
    })?;

    if let Some(total_unique) = query_iter.flatten().next() {
        stats.terms_unique = total_unique as u64;
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use uuid::Uuid;
    use crate::snip;
    use crate::snip::test_prep::*;

    #[test]
    fn test_get_term_context() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        let id = Uuid::try_parse(ID_STR)?;
        let mut s = snip::get_from_uuid(&conn, &id)?;
        s.analyze()?;
        // println!("{}", s.text);

        let position = 3;
        let term = &s.analysis.words[position].word;
        let expect: Vec<usize> = vec![0, 1, 2, 3, 4, 5, 6];
        let context = s.analysis.get_term_context_positions(position, 3);
        println!("context: {:?}", context);
        assert_eq!(expect, context);

        // print context
        let context_full: Vec<&SnipWord> = s.analysis.get_term_context_words(context);
        for c in context_full.iter() {
            // print first word
            if c.word == *term {
                print!("[{}]", c.word);
            } else {
                print!("{}", c.word);
            }
            if let Some(suffix) = &c.suffix {
                print!("{}", suffix);
            }
        }

        Ok(())
    }

    #[test]
    fn test_stats_index() -> Result<(), Box<dyn Error>> {
        let conn = prepare_database().expect("preparing in-memory database");
        snip::index_all_items(&conn)?;

        let stats = stats_index(&conn)?;
        println!("terms_total: {}", stats.terms_total);
        println!("terms_unique: {}", stats.terms_unique);

        Ok(())
    }
}