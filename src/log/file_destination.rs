//! File log destination.

use super::{LogDestination, LogLevel};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Log destination that writes to a file.
pub struct FileDestination {
    file_path: PathBuf,
    writer: Mutex<File>,
}

impl FileDestination {
    /// Creates a new file destination.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the log file
    pub fn new<P: AsRef<Path>>(file_path: P) -> std::io::Result<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        Ok(Self {
            file_path,
            writer: Mutex::new(file),
        })
    }

    /// Creates a file destination in the data directory's logs folder.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path to the data directory
    /// * `command_name` - Name of the command (used in filename)
    pub fn from_data_dir<P: AsRef<Path>>(
        data_dir: P,
        command_name: &str,
    ) -> std::io::Result<Self> {
        let log_dir = data_dir.as_ref().join("logs");
        fs::create_dir_all(&log_dir)?;

        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("{}_{}.log", timestamp, command_name);
        let file_path = log_dir.join(filename);

        Self::new(file_path)
    }

    /// Gets the path to the log file.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }
}

impl LogDestination for FileDestination {
    fn write(&self, _level: LogLevel, message: &str) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writeln!(writer, "{}", message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_file_destination_creation() {
        let temp_dir = env::temp_dir().join("rstproto_test_log");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let log_file = temp_dir.join("test.log");
        let dest = FileDestination::new(&log_file).unwrap();

        dest.write(LogLevel::Info, "Test message");

        assert!(log_file.exists());
        let contents = fs::read_to_string(&log_file).unwrap();
        assert!(contents.contains("Test message"));

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
