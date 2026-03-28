
use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::pds::PdsDb;


pub fn cmd_sync_repo(args: &HashMap<String, String>) {
    let log = logger();
    log.info("SyncRepo command started");

    let source_data_dir = match get_arg(args, "sourcedatadir") {
        Some(d) => d,
        None => {
            log.error("missing /sourceDataDir argument");
            log.error("Usage: rustproto /command SyncRepo /sourceDataDir <path> /destDataDir <path>");
            return;
        }
    };

    let dest_data_dir = match get_arg(args, "destdatadir") {
        Some(d) => d,
        None => {
            log.error("missing /destDataDir argument");
            log.error("Usage: rustproto /command SyncRepo /sourceDataDir <path> /destDataDir <path>");
            return;
        }
    };

    // Initialize file systems
    let source_lfs = match LocalFileSystem::initialize(source_data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize source file system: {}", e));
            return;
        }
    };

    let dest_lfs = match LocalFileSystem::initialize(dest_data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize dest file system: {}", e));
            return;
        }
    };

    // Connect to databases
    let source_db = match PdsDb::connect(&source_lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to source database: {}", e));
            return;
        }
    };

    let dest_db = match PdsDb::connect(&dest_lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to dest database: {}", e));
            return;
        }
    };

    // =========================================================================
    // SYNC REPO HEADER
    // =========================================================================
    log.info("");
    log.info("=== SYNC REPO HEADER ===");
    let source_header = match source_db.get_repo_header() {
        Ok(h) => h,
        Err(e) => {
            log.error(&format!("Failed to get source repo header: {}", e));
            return;
        }
    };
    log.info(&format!("Source RepoHeader: commitCid={} version={}", source_header.repo_commit_cid, source_header.version));

    if let Err(e) = dest_db.delete_repo_header() {
        log.error(&format!("Failed to delete dest repo header: {}", e));
        return;
    }
    if let Err(e) = dest_db.insert_update_repo_header(&source_header) {
        log.error(&format!("Failed to insert dest repo header: {}", e));
        return;
    }
    log.info("RepoHeader synced.");

    // =========================================================================
    // SYNC REPO COMMIT
    // =========================================================================
    log.info("");
    log.info("=== SYNC REPO COMMIT ===");
    let source_commit = match source_db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get source repo commit: {}", e));
            return;
        }
    };
    log.info(&format!("Source RepoCommit: cid={} rev={} rootMst={}", source_commit.cid, source_commit.rev, source_commit.root_mst_node_cid));

    if let Err(e) = dest_db.delete_repo_commit() {
        log.error(&format!("Failed to delete dest repo commit: {}", e));
        return;
    }
    if let Err(e) = dest_db.insert_update_repo_commit(&source_commit) {
        log.error(&format!("Failed to insert dest repo commit: {}", e));
        return;
    }
    log.info("RepoCommit synced.");

    // =========================================================================
    // SYNC REPO RECORDS
    // =========================================================================
    log.info("");
    log.info("=== SYNC REPO RECORDS ===");
    let source_records = match source_db.get_all_repo_records() {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Failed to get source repo records: {}", e));
            return;
        }
    };
    log.info(&format!("Source records: {}", source_records.len()));

    // Delete all existing dest records
    if let Err(e) = dest_db.delete_all_repo_records() {
        log.error(&format!("Failed to delete dest repo records: {}", e));
        return;
    }

    // Insert source records into dest
    let mut records_synced = 0;
    for record in &source_records {
        if let Err(e) = dest_db.insert_repo_record(
            &record.collection,
            &record.rkey,
            &record.cid,
            &record.dag_cbor_bytes,
        ) {
            log.error(&format!("Failed to insert record {}/{}: {}", record.collection, record.rkey, e));
            return;
        }
        records_synced += 1;
    }
    log.info(&format!("RepoRecords synced: {}", records_synced));

    // =========================================================================
    // SYNC BLOBS (database metadata)
    // =========================================================================
    log.info("");
    log.info("=== SYNC BLOBS ===");
    let source_blobs = match source_db.get_all_blobs() {
        Ok(b) => b,
        Err(e) => {
            log.error(&format!("Failed to get source blobs: {}", e));
            return;
        }
    };
    log.info(&format!("Source blobs: {}", source_blobs.len()));

    // Delete all existing dest blob metadata
    if let Err(e) = dest_db.delete_all_blobs() {
        log.error(&format!("Failed to delete dest blobs: {}", e));
        return;
    }

    // Insert source blob metadata into dest
    let mut blobs_synced = 0;
    for blob in &source_blobs {
        if let Err(e) = dest_db.insert_blob(blob) {
            log.error(&format!("Failed to insert blob {}: {}", blob.cid, e));
            return;
        }
        blobs_synced += 1;
    }
    log.info(&format!("Blob metadata synced: {}", blobs_synced));

    // =========================================================================
    // SYNC BLOB FILES (on disk)
    // =========================================================================
    log.info("");
    log.info("=== SYNC BLOB FILES ===");
    let source_blob_db = crate::pds::blob_db::BlobDb::new(&source_lfs, log);
    let dest_blob_db = crate::pds::blob_db::BlobDb::new(&dest_lfs, log);

    let mut blob_files_synced = 0;
    let mut blob_files_skipped = 0;
    for blob in &source_blobs {
        if !source_blob_db.has_blob_bytes(&blob.cid) {
            log.info(&format!("Source blob file missing, skipping: {}", blob.cid));
            blob_files_skipped += 1;
            continue;
        }

        let bytes = match source_blob_db.get_blob_bytes(&blob.cid) {
            Ok(b) => b,
            Err(e) => {
                log.error(&format!("Failed to read source blob file {}: {}", blob.cid, e));
                return;
            }
        };

        // Write blob to dest (overwrite if exists)
        if let Err(e) = dest_blob_db.insert_blob_bytes(&blob.cid, &bytes) {
            log.error(&format!("Failed to write dest blob file {}: {}", blob.cid, e));
            return;
        }
        blob_files_synced += 1;
    }
    log.info(&format!("Blob files synced: {}, skipped: {}", blob_files_synced, blob_files_skipped));

    // =========================================================================
    // SUMMARY
    // =========================================================================
    log.info("");
    log.info("=== SYNC COMPLETE ===");
    log.info(&format!("RepoHeader:  synced"));
    log.info(&format!("RepoCommit:  synced"));
    log.info(&format!("RepoRecords: {} synced", records_synced));
    log.info(&format!("Blobs:       {} metadata synced", blobs_synced));
    log.info(&format!("Blob files:  {} synced, {} skipped", blob_files_synced, blob_files_skipped));
}


