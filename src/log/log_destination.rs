//! Log destination trait.

use super::LogLevel;

/// Trait for log destinations (console, file, etc.)
pub trait LogDestination: Send + Sync {
    /// Write a log message at the given level.
    fn write(&self, level: LogLevel, message: &str);
}
