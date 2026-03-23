use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
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

    ///
    /// Writes a repository to a stream.
    ///
    pub fn write_repo<W: Write>(
        writer: W,
        header: &RepoHeader,
        records: &[RepoRecord],
    ) -> io::Result<()> {
        let mut buf_writer = BufWriter::new(writer);

        // Write header
        header.write_to_stream(&mut buf_writer)?;

        // Write records
        for record in records {
            record.write_to_stream(&mut buf_writer)?;
        }

        buf_writer.flush()?;
        Ok(())
    }

    /// Writes a repository to a file.
    pub fn write_repo_file<P: AsRef<Path>>(
        path: P,
        header: &RepoHeader,
        records: &[RepoRecord],
    ) -> io::Result<()> {
        let file = File::create(path)?;
        Self::write_repo(file, header, records)
    }

    /// Reads a repository from a stream into memory.
    ///
    /// Returns the header and all records.
    pub fn read_repo<R: Read>(reader: R) -> io::Result<(RepoHeader, Vec<RepoRecord>)> {
        let mut buf_reader = BufReader::new(reader);

        let header = RepoHeader::read_from_stream(&mut buf_reader)?;
        let mut records = Vec::new();

        loop {
            match RepoRecord::read_from_stream(&mut buf_reader) {
                Ok(record) => records.push(record),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok((header, records))
    }

    /// Reads a repository from a file into memory.
    pub fn read_repo_file<P: AsRef<Path>>(path: P) -> io::Result<(RepoHeader, Vec<RepoRecord>)> {
        let file = File::open(path)?;
        Self::read_repo(file)
    }

    /// Copies a repository from one stream to another.
    /// This reads the repository, then writes it back out.
    pub fn copy_repo<R: Read, W: Write>(reader: R, writer: W) -> io::Result<()> {
        let (header, records) = Self::read_repo(reader)?;
        Self::write_repo(writer, &header, &records)
    }

    /// Copies a repository file to another file.
    pub fn copy_repo_file<P1: AsRef<Path>, P2: AsRef<Path>>(
        input_path: P1,
        output_path: P2,
    ) -> io::Result<()> {
        let input_file = File::open(input_path)?;
        let output_file = File::create(output_path)?;
        Self::copy_repo(input_file, output_file)
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
