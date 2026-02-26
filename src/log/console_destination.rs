//! Console log destination.

use super::{LogDestination, LogLevel};

/// Log destination that writes to the console.
pub struct ConsoleDestination {
    use_colors: bool,
}

impl ConsoleDestination {
    /// Creates a new console destination with colors enabled.
    pub fn new() -> Self {
        Self { use_colors: true }
    }

    /// Creates a new console destination with configurable colors.
    pub fn with_colors(use_colors: bool) -> Self {
        Self { use_colors }
    }
}

impl Default for ConsoleDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl LogDestination for ConsoleDestination {
    fn write(&self, level: LogLevel, message: &str) {
        if self.use_colors {
            match level {
                LogLevel::Trace => println!("{}", message),
                LogLevel::Info => println!("{}", message),
                LogLevel::Warning => {
                    // Yellow for warnings
                    println!("\x1b[33m{}\x1b[0m", message);
                }
                LogLevel::Error => {
                    // Red for errors
                    eprintln!("\x1b[31m{}\x1b[0m", message);
                }
            }
        } else {
            match level {
                LogLevel::Error => eprintln!("{}", message),
                _ => println!("{}", message),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_console_destination_creation() {
        let dest = ConsoleDestination::new();
        assert!(dest.use_colors);

        let dest = ConsoleDestination::with_colors(false);
        assert!(!dest.use_colors);
    }
}
