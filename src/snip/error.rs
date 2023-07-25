use std::error::Error;
use std::fmt;

/// Errors for Snip Analysis
pub enum SnipError {
    Analysis(String),
    UuidNotFound(String),
}

impl Error for SnipError {}

impl fmt::Display for SnipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnipError::Analysis(s) => write!(f, "Analysis encountered an error: {}", s),
            SnipError::UuidNotFound(s) => write!(f, "uuid {} was not found", s),
        }
    }
}

impl fmt::Debug for SnipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnipError::Analysis(s) => write!(
                f,
                "{{ SnipError::Analysis({}) file: {}, line: {} }}",
                s,
                file!(),
                line!()
            ),
            SnipError::UuidNotFound(s) => write!(
                f,
                "{{ SnipError::UuidNotFound({}) file: {}, line: {} }}",
                s,
                file!(),
                line!()
            ),
        }
    }
}
