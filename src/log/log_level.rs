//! Log level enumeration.

use std::fmt;
use std::str::FromStr;

/// Log levels for filtering messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// Most verbose - detailed debugging information
    Trace = 0,
    /// Standard informational messages
    Info = 1,
    /// Warning messages for potential issues
    Warning = 2,
    /// Error messages (always shown)
    Error = 3,
}

impl Default for LogLevel {
    fn default() -> Self {
        LogLevel::Info
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warning => write!(f, "WARNING"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "trace" => Ok(LogLevel::Trace),
            "info" => Ok(LogLevel::Info),
            "warning" | "warn" => Ok(LogLevel::Warning),
            "error" => Ok(LogLevel::Error),
            _ => Ok(LogLevel::Info), // Default to info for unknown values
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warning);
        assert!(LogLevel::Warning < LogLevel::Error);
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("trace").unwrap(), LogLevel::Trace);
        assert_eq!(LogLevel::from_str("INFO").unwrap(), LogLevel::Info);
        assert_eq!(LogLevel::from_str("Warning").unwrap(), LogLevel::Warning);
        assert_eq!(LogLevel::from_str("error").unwrap(), LogLevel::Error);
        assert_eq!(LogLevel::from_str("unknown").unwrap(), LogLevel::Info);
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(format!("{}", LogLevel::Trace), "TRACE");
        assert_eq!(format!("{}", LogLevel::Info), "INFO");
        assert_eq!(format!("{}", LogLevel::Warning), "WARNING");
        assert_eq!(format!("{}", LogLevel::Error), "ERROR");
    }
}
