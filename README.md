<div align="center">
  <img src="logo.png" alt="Immich Sync Service" width="200">
  <h1>Immich Sync Service</h1>
  <p>Bi-directional file synchronization for <a href="https://github.com/immich-app/immich">Immich</a>.</p>
</div>

---

## About

This is a service that watches a folder and keeps it in sync with [Immich](https://github.com/immich-app/immich). Drop a file in the folder and it shows up in Immich. Delete it locally and it's gone from Immich too. It works the other way around as well, delete something in Immich and the local file is removed.

It picks up new and modified files as they appear, and does a full scan once a day to catch anything it missed. Files are never uploaded twice. You can configure multiple users, each with their own folder. The service only talks to the Immich API, no direct database access needed.

Deletion behavior is guarded by policy controls (`delete_threshold` and `delete_max_age`) to reduce accidental data loss from propagated deletes.

## Use Case

I use this to sync the images on my phone with my Immich instance. I sync photos from my phone to a folder on my server with Syncthing. From that folder, this service picks up the files and imports them into Immich.

The nice thing is that deletes flow through the entire chain. If I delete an image on my phone, Syncthing removes it from the server, and this service deletes it from Immich. If I delete an image in Immich, this service removes the local copy, and it disappears from my phone.

There are drawbacks. Images are stored twice, once in the sync folder and again in Immich. For a phone-sized collection that's fine. Bi-directional deletes also make you vulnerable to accidental deletions. If your phone, server or Immich deletes all images, that deletion will propagate to **all three places**. So have backups! Deleting in Immich does move images to the trash first, so there is a recovery window.

I can use any photo app on my phone, no Immich mobile client required. And I can manage my collection from Immich on my computer to free up space on my phone. That makes it worth it for me.

There are built-in protections to reduce the risk. The `delete_threshold` setting controls how old a file needs to be before a local delete is ignored. Remove a photo that has been in Immich longer than the threshold and it just stays there. Files without a known creation date are never deleted from Immich at all.

I use this to automatically clean up old files. I have a job that removes files older than 600 days from my server, which also cleans them off my phone. But `delete_threshold` is set to 14 days, so those old deletions are ignored and the photos stay safe in Immich.

## Quick Start

### Prerequisites

- A running Immich instance
- An Immich API key with the following permissions: `asset.upload`, `asset.read`, `asset.delete`

### Supported Immich versions

Both Immich v2 and v3 are supported, and CI tests against both. Immich v2
support is deprecated: the service logs a warning at startup when it detects
a v2 server, and 0.3.x releases will drop v2 support entirely. Upgrade your
Immich server to v3 to stay on the supported path.

### Install

1. Go to [Releases](https://github.com/nsg/immich-sync/releases/latest).
2. Download the archive for your operating system and CPU architecture.
3. Extract the archive and place the `sync-service` binary (or `sync-service.exe` on Windows) somewhere in your `PATH`, or run it from the extracted folder.

### Configure

Create a `config.toml` file:

```toml
database_path = "/var/lib/sync-service/sync-service.db"

# Optional: path to structured JSONL event log
event_log = "/var/log/sync-service/events.jsonl"

[immich]
server_url = "http://localhost:2283"

# Local deletes younger than this (days) are propagated to Immich
delete_threshold = 365

# Files older than this (days) are considered invalid and skipped
delete_max_age = 3650

# Seconds between deletion reconciliation checks
delete_poll_interval = 3600

# Seconds between full directory scans
import_poll_interval = 86400 

# Seconds between upload queue checks
upload_poll_interval = 60

[[user]]
user_id = "uuid-here"
user_key = "api-key-here"
path = "/data/photos/user1"
```

### Run

```bash
./sync-service --config /my/path/config.toml
```

Windows (PowerShell):

```powershell
.\sync-service.exe --config C:\path\to\config.toml
```

If no path is provided, the service looks for `config.toml` in the current directory.

Place files in the configured `path` directory and they will be uploaded to Immich.

### Dry Run

Use `--dry-run` (or `-n`) to simulate without making any changes:

```bash
./sync-service -n --config /my/path/config.toml
```

The service runs normally — discovering files, hashing, and checking the Immich API — but
skips all uploads, Immich deletes, and local file deletes. Each skipped action is recorded
in the event log (requires `event_log` to be configured) with `detail: "dry-run"` so you
can verify what would happen before running it for real.

> **Note:** The local database is still populated during a dry run so that discovery and
> reconciliation behave realistically. Delete the database file after a dry run to avoid
> stale state affecting a subsequent normal run.

### Event Log

If `event_log` is configured, the service writes one JSON object per line (JSONL)
for worker events such as scans, uploads, and delete propagation decisions.
Fields include `timestamp`, `worker`, `event`, `user_id`, and optional metadata
(`path`, `asset_id`, `detail`).

## How It Works

Four workers run per configured user:

1. **File Watcher** monitors the sync directory for changes in real-time. New or modified files are uploaded, deleted files are removed from Immich if they are younger than the threshold.

2. **Import Watcher** runs a full directory scan once a day. Picks up anything the file watcher missed, like files that existed before the service started.

3. **Upload Worker** checks the local queue for files that do not have an Immich asset ID yet. It uses `bulk-upload-check` to deduplicate and either links existing assets or uploads new files.

4. **Deletion Watcher** checks periodically if any assets have been deleted from Immich and removes the local files.

## Tests

There are unit tests and CI runs integration tests against the latest release of immich-distribution to catch regressions.

## History

This is a rewrite of the Python sync component from [immich-distribution](https://github.com/nsg/immich-distribution), my Snap-packaged distribution of Immich. The original connected directly to Immich's PostgreSQL database with custom tables and a deletion trigger. That was necessary at the time because the API lacked the endpoints I needed, but it also made it tightly coupled to immich-distribution.

The API has matured since then. This rewrite talks to the Immich API only and uses a local SQLite file for bookkeeping. It works with any Immich deployment, not just my Snap package.

## License

MIT
