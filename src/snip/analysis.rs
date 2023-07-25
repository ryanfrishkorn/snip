use std::error::Error;

/// Analysis of the document derived from
#[derive(Debug)]
pub struct SnipAnalysis {
    pub words: Vec<SnipWord>,
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
