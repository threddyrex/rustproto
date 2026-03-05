//! File log destination.

use super::{LogDestination, LogLevel};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Maximum log file size before rolling (10 MB).
const MAX_LOG_SIZE: usize = 10 * 1024 * 1024;

/// Internal state protected by a single mutex.
struct FileState {
    writer: File,
    log_length: usize,
}

/// Log destination that writes to a file.
/// Rolls the log file when it exceeds MAX_LOG_SIZE.
pub struct FileDestination {
    file_path: PathBuf,
    state: Mutex<FileState>,
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

        let initial_length = if file_path.exists() {
            fs::metadata(&file_path).map(|m| m.len() as usize).unwrap_or(0)
        } else {
            0
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        Ok(Self {
            file_path,
            state: Mutex::new(FileState {
                writer: file,
                log_length: initial_length,
            }),
        })
    }

    /// Creates a file destination in the data directory's logs folder.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path to the data directory
    /// * `command_name` - Name of the command (used in filename)
    /// * `log_filename` - Optional custom filename (if None, generates timestamp-based name)
    pub fn from_data_dir<P: AsRef<Path>>(
        data_dir: P,
        command_name: &str,
        log_filename: Option<&str>,
    ) -> std::io::Result<Self> {
        let log_dir = data_dir.as_ref().join("logs");
        fs::create_dir_all(&log_dir)?;

        let filename = match log_filename {
            Some(name) => name.to_string(),
            None => {
                let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
                format!("{}_{}.log", timestamp, command_name)
            }
        };
        let file_path = log_dir.join(filename);

        Self::new(file_path)
    }

    /// Gets the path to the log file.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Rolls the log file if it exceeds MAX_LOG_SIZE.
    /// Renames the current file with a timestamp `.bak` suffix and opens a new file.
    /// Must be called while `state` is already locked (lock passed in).
    fn check_roll_log(&self, state: &mut FileState) {
        if state.log_length < MAX_LOG_SIZE {
            return;
        }

        // Write roll marker and flush before renaming
        let _ = writeln!(state.writer, " --- Rolling log file due to size limit --- ");
        let _ = state.writer.flush();

        // Rename old file
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let bak_path = format!("{}.{}.bak", self.file_path.display(), timestamp);
        let _ = fs::rename(&self.file_path, &bak_path);

        // Open new file and reset length
        if let Ok(new_file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
        {
            state.writer = new_file;
            state.log_length = 0;
        }
    }
}

impl LogDestination for FileDestination {
    fn write(&self, _level: LogLevel, message: &str) {
        if let Ok(mut state) = self.state.lock() {
            let _ = writeln!(state.writer, "{}", message);
            state.log_length += message.len();
            self.check_roll_log(&mut state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_file_destination_creation() {
        let temp_dir = env::temp_dir().join("rustproto_test_log");
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
