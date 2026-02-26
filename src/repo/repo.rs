//! Repository walking functionality.
//!
//! Provides methods to walk through a CAR file and process each record.
//!
//! CAR file format (from spec):
//! ```text
//! [---  header  -------- ]   [----------------- data ---------------------------------]
//! [varint | header block ]   [varint | cid | data block]....[varint | cid | data block]
//! ```
//!
//! Reference:
//! - https://ipld.io/specs/transport/car/carv1/
//! - https://atproto.com/specs/repository

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use super::repo_header::RepoHeader;
use super::repo_record::RepoRecord;

/// Repository walker for CAR files.
pub struct Repo;

impl Repo {
    /// Walks through a repository stream, calling callbacks for header and each record.
    ///
    /// The header callback receives the RepoHeader and should return `true` to continue
    /// processing or `false` to stop.
    ///
    /// The record callback receives each RepoRecord and should return `true` to continue
    /// processing or `false` to stop.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rstproto::repo::Repo;
    /// use std::fs::File;
    ///
    /// let file = File::open("repo.car").unwrap();
    /// Repo::walk_repo(file, |header| {
    ///     println!("Version: {}", header.version);
    ///     true
    /// }, |record| {
    ///     println!("Record: {:?}", record.at_proto_type);
    ///     true
    /// }).unwrap();
    /// ```
    pub fn walk_repo<R, FH, FR>(
        reader: R,
        header_callback: FH,
        record_callback: FR,
    ) -> io::Result<()>
    where
        R: Read,
        FH: FnOnce(&RepoHeader) -> bool,
        FR: FnMut(&RepoRecord) -> bool,
    {
        Self::walk_repo_inner(reader, header_callback, record_callback)
    }

    fn walk_repo_inner<R, FH, FR>(
        reader: R,
        header_callback: FH,
        mut record_callback: FR,
    ) -> io::Result<()>
    where
        R: Read,
        FH: FnOnce(&RepoHeader) -> bool,
        FR: FnMut(&RepoRecord) -> bool,
    {
        let mut buf_reader = BufReader::new(reader);

        // Read header
        let repo_header = RepoHeader::read_from_stream(&mut buf_reader)?;
        let keep_going = header_callback(&repo_header);

        if !keep_going {
            return Ok(());
        }

        // Read records until EOF
        loop {
            match RepoRecord::read_from_stream(&mut buf_reader) {
                Ok(record) => {
                    let keep_going = record_callback(&record);
                    if !keep_going {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Normal end of file
                    break;
                }
                Err(e) => {
                    // Propagate other errors
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Walks through a repository file, calling callbacks for header and each record.
    pub fn walk_repo_file<P, FH, FR>(
        path: P,
        header_callback: FH,
        record_callback: FR,
    ) -> io::Result<()>
    where
        P: AsRef<Path>,
        FH: FnOnce(&RepoHeader) -> bool,
        FR: FnMut(&RepoRecord) -> bool,
    {
        let file = File::open(path)?;
        Self::walk_repo(file, header_callback, record_callback)
    }

    /// Returns an iterator over records in a repository file.
    /// This is useful when you want to process records lazily.
    pub fn iter_records<R: Read>(reader: R) -> io::Result<RepoIterator<R>> {
        let mut buf_reader = BufReader::new(reader);

        // Read header first
        let header = RepoHeader::read_from_stream(&mut buf_reader)?;

        Ok(RepoIterator {
            reader: buf_reader,
            header,
            done: false,
        })
    }
}

/// An iterator over records in a repository.
pub struct RepoIterator<R: Read> {
    reader: BufReader<R>,
    header: RepoHeader,
    done: bool,
}

impl<R: Read> RepoIterator<R> {
    /// Returns a reference to the header.
    pub fn header(&self) -> &RepoHeader {
        &self.header
    }
}

impl<R: Read> Iterator for RepoIterator<R> {
    type Item = io::Result<RepoRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match RepoRecord::read_from_stream(&mut self.reader) {
            Ok(record) => Some(Ok(record)),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full integration tests would require a real CAR file
    // These are placeholder tests for the API design

    #[test]
    fn test_repo_struct_exists() {
        // Just verify the API compiles
        let _repo = Repo;
    }
}
