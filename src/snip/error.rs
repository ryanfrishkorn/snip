use std::error::Error;
use std::fmt;

/// Custom Errors
pub enum SnipError {
    Analysis(String),
    General(String),
    SearchNoMatches(String),
    UuidMultipleMatches(String),
    UuidNotFound(String),
}

impl Error for SnipError {}

impl fmt::Display for SnipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnipError::Analysis(s) => write!(f, "{:?}: {}", self, s),
            SnipError::General(s) => write!(f, "{:?}: {}", self, s),
            SnipError::SearchNoMatches(s) => write!(f, "{:?}: {}", self, s),
            SnipError::UuidMultipleMatches(s) => write!(f, "{:?}: {}", self, s),
            SnipError::UuidNotFound(s) => write!(f, "The id requested was not found: {}", s),
        }
    }
}

impl fmt::Debug for SnipError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SnipError::Analysis(s) => f
                .debug_struct("SnipError::Analysis")
                .field("msg", s)
                .finish(),
            SnipError::General(s) => f
                .debug_struct("SnipError::General")
                .field("msg", s)
                .finish(),
            SnipError::SearchNoMatches(s) => f
                .debug_struct("SnipError::SearchNoMatches")
                .field("msg", s)
                .finish(),
            SnipError::UuidMultipleMatches(s) => f
                .debug_struct("SnipError::UuidMultipleMatches")
                .field("msg", s)
                .finish(),
            SnipError::UuidNotFound(s) => f
                .debug_struct("SnipError::UuidNotFound")
                .field("msg", s)
                .finish(),
        }
    }
}
