//! Logging module for rustproto.
//!
//! This module provides logging functionality with configurable log levels
//! and multiple destinations (console, file).

mod console_destination;
mod file_destination;
mod log_destination;
mod log_level;
mod logger;

pub use console_destination::ConsoleDestination;
pub use file_destination::FileDestination;
pub use log_destination::LogDestination;
pub use log_level::LogLevel;
pub use logger::{init_logger, logger, Logger};
