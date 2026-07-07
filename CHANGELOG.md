# Changelog

All notable changes to this project will be documented in this file.

## 0.2.0 - 2026-07-07

### Added

- Support for Immich v3. CI now runs the integration test suite against both
  the latest v2 release and the latest v3 release.
- The Immich server version is logged at startup.

### Changed

- Asset uploads send RFC 3339 timestamps (`2024-01-15T12:30:00Z`), required
  by Immich v3's stricter validation and accepted by v2. Existing local
  databases are unaffected.

### Deprecated

- Immich v2 support. The service logs a warning at startup when it detects a
  v2 server. Support for Immich v2 will be removed in 0.3.x releases.

## 0.1.6 - 2026-03-13

### Fixed

- Set `created_at` when discovering and watching files. The discovery and file watcher workers were inserting assets without a creation timestamp, so delete propagation was skipped for any asset that hadn't been re-uploaded. Existing rows are backfilled on the next discovery scan.

## 0.1.5 - 2026-03-13

### Fixed

- Handle Syncthing rename-to-trash deletions. Syncthing deletes files by renaming them to `.trashed-*` instead of unlinking, which was not detected as a removal. The file watcher now treats Create/Modify events for missing files as deletions.
- Reduce log noise from deletion_watcher cleanup races (suppress "not found in local database" for already-removed records).

## 0.1.4 - 2026-03-13

### Added

- Debounce file watcher events to avoid redundant hashes and uploading partially-written files. Events for the same path are coalesced over a 2-second window before processing.

## 0.1.3 - 2026-03-12

### Fixed

- Build Linux releases against multiple glibc versions for broader compatibility.

## 0.1.2 - 2026-03-12

### Added

- `--dry-run` (`-n`) mode that skips all mutations (uploads, deletes, database writes).

## 0.1.1 - 2026-02-27

### Added

- Structured JSONL event log for observability across all workers.

### Changed

- Use worker name constants and improve event log error handling and parsing.

## 0.1.0 - 2026-02-17

Initial release of Immich Sync Service.
