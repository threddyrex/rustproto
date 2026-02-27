//! SQLite database connection helpers.
//!
//! This module provides functions for creating SQLite database connections
//! with different modes (read-only, read-write, read-write-create).

use rusqlite::{Connection, OpenFlags};
use std::path::Path;

/// Helper struct for SQLite database connections.
pub struct SqliteDb;

impl SqliteDb {
    /// Get a read/write connection to an existing database.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the SQLite database file
    ///
    /// # Returns
    ///
    /// A Connection to the database, or an error if the connection fails.
    pub fn get_connection<P: AsRef<Path>>(db_path: P) -> Result<Connection, rusqlite::Error> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Connection::open_with_flags(db_path, flags)
    }

    /// Get a read/write/create connection to a database, creating it if it does not exist.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the SQLite database file
    ///
    /// # Returns
    ///
    /// A Connection to the database, or an error if the connection fails.
    pub fn get_connection_create<P: AsRef<Path>>(db_path: P) -> Result<Connection, rusqlite::Error> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Connection::open_with_flags(db_path, flags)
    }

    /// Get a read-only connection to an existing database.
    ///
    /// # Arguments
    ///
    /// * `db_path` - Path to the SQLite database file
    ///
    /// # Returns
    ///
    /// A Connection to the database, or an error if the connection fails.
    pub fn get_connection_read_only<P: AsRef<Path>>(
        db_path: P,
    ) -> Result<Connection, rusqlite::Error> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Connection::open_with_flags(db_path, flags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create_and_connect() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create connection should create the file
        let conn = SqliteDb::get_connection_create(&db_path).unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", [])
            .unwrap();
        drop(conn);

        assert!(db_path.exists());

        // Regular connection should work
        let conn = SqliteDb::get_connection(&db_path).unwrap();
        conn.execute("INSERT INTO test (id) VALUES (1)", [])
            .unwrap();
        drop(conn);

        // Read-only connection should work
        let conn = SqliteDb::get_connection_read_only(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
