//! Main logger implementation.

use super::{ConsoleDestination, LogDestination, LogLevel};
use std::sync::{Arc, RwLock};

/// Main logger struct that handles log levels and destinations.
pub struct Logger {
    level: RwLock<LogLevel>,
    destinations: RwLock<Vec<Arc<dyn LogDestination>>>,
}

impl Logger {
    /// Creates a new logger with the given log level and a console destination.
    pub fn new(level: LogLevel) -> Self {
        let logger = Self {
            level: RwLock::new(level),
            destinations: RwLock::new(Vec::new()),
        };
        logger.add_destination(Arc::new(ConsoleDestination::new()));
        logger
    }

    /// Creates a new logger with default settings (Info level, console output).
    pub fn default_logger() -> Self {
        Self::new(LogLevel::Info)
    }

    /// Sets the log level.
    pub fn set_level(&self, level: LogLevel) {
        if let Ok(mut lvl) = self.level.write() {
            *lvl = level;
        }
    }

    /// Gets the current log level.
    pub fn level(&self) -> LogLevel {
        *self.level.read().unwrap_or_else(|_| panic!("Logger lock poisoned"))
    }

    /// Adds a log destination.
    pub fn add_destination(&self, destination: Arc<dyn LogDestination>) {
        if let Ok(mut dests) = self.destinations.write() {
            dests.push(destination);
        }
    }

    /// Gets the current timestamp string.
    fn get_timestamp() -> String {
        chrono::Local::now().format("[%Y-%m-%d %H:%M:%S%.3f]").to_string()
    }

    /// Logs a message at the given level.
    fn log(&self, level: LogLevel, message: &str) {
        let current_level = self.level();
        if level >= current_level {
            let timestamp = Self::get_timestamp();
            let full_message = format!("{} [{}] {}", timestamp, level, message);

            if let Ok(dests) = self.destinations.read() {
                for dest in dests.iter() {
                    dest.write(level, &full_message);
                }
            }
        }
    }

    /// Logs a trace message.
    pub fn trace(&self, message: &str) {
        self.log(LogLevel::Trace, message);
    }

    /// Logs an info message.
    pub fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }

    /// Logs a warning message.
    pub fn warning(&self, message: &str) {
        self.log(LogLevel::Warning, message);
    }

    /// Logs an error message.
    pub fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
    }
}

impl Default for Logger {
    fn default() -> Self {
        Self::default_logger()
    }
}

// Global logger instance
use std::sync::OnceLock;

static GLOBAL_LOGGER: OnceLock<Logger> = OnceLock::new();

/// Initializes the global logger.
///
/// # Arguments
///
/// * `level` - The log level to use
///
/// # Returns
///
/// A reference to the global logger
pub fn init_logger(level: LogLevel) -> &'static Logger {
    GLOBAL_LOGGER.get_or_init(|| Logger::new(level))
}

/// Gets the global logger, initializing with defaults if needed.
pub fn logger() -> &'static Logger {
    GLOBAL_LOGGER.get_or_init(Logger::default_logger)
}

/// Convenience macro for logging at trace level.
#[macro_export]
macro_rules! log_trace {
    ($($arg:tt)*) => {
        $crate::log::logger().trace(&format!($($arg)*))
    };
}

/// Convenience macro for logging at info level.
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::log::logger().info(&format!($($arg)*))
    };
}

/// Convenience macro for logging at warning level.
#[macro_export]
macro_rules! log_warning {
    ($($arg:tt)*) => {
        $crate::log::logger().warning(&format!($($arg)*))
    };
}

/// Convenience macro for logging at error level.
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::log::logger().error(&format!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logger_creation() {
        let logger = Logger::new(LogLevel::Trace);
        assert_eq!(logger.level(), LogLevel::Trace);
    }

    #[test]
    fn test_logger_set_level() {
        let logger = Logger::new(LogLevel::Info);
        assert_eq!(logger.level(), LogLevel::Info);

        logger.set_level(LogLevel::Warning);
        assert_eq!(logger.level(), LogLevel::Warning);
    }
}
