use std::error::Error;
use std::fmt;

/// Errors for Snip Analysis
pub enum SnipError {
    Analysis(String),
    General(String),
    UuidMultipleMatches(String),
    SearchNoMatches(String),
    UuidNotFound(String),
}

impl Error for SnipError {}

impl fmt::Display for SnipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnipError::Analysis(s) => write!(f, "Analysis encountered an error: {}", s),
            SnipError::General(s) => write!(f, "{}", s),
            SnipError::UuidMultipleMatches(s) => write!(f, "{}", s),
            SnipError::SearchNoMatches(s) => write!(f, "{}", s),
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
            SnipError::General(s) => write!(
                f,
                "{{ SnipError::General({}) file: {}, line: {} }}",
                s,
                file!(),
                line!()
            ),
            SnipError::UuidMultipleMatches(s) => write!(
                f,
                "{{ SnipError::UuidMultipleMatches({}) file: {}, line: {} }}",
                s,
                file!(),
                line!()
            ),
            SnipError::SearchNoMatches(s) => write!(
                f,
                "{{ SnipError::NoIndexMatches({}) file: {}, line: {} }}",
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
