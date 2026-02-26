# Data Directory

This directory contains local data for rstproto, including:

- **actors/** - Cached actor information (JSON files)
- **repos/** - Downloaded repository CAR files
- **sessions/** - Session files with access tokens
- **preferences/** - User preferences
- **backups/** - Account backups
- **pds/** - PDS-related data
  - **blobs/** - Downloaded blobs
- **scratch/** - Temporary files
- **logs/** - Log files
- **records/** - Individual record data

## Usage

The data directory is created and managed automatically by rstproto commands.
To initialize manually, use the `LocalFileSystem::initialize_with_create()` function
or run any command with the `/dataDir` argument:

```powershell
.\GetRepo.ps1 -actor "alice.bsky.social" -dataDir "..\data"
```

## Note

This directory's contents are ignored by git (except this README).
Do not commit sensitive data like session files or access tokens.
