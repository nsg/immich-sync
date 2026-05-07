use crate::event_log::{workers, EventLogger};
use crate::hash::hash_file;
use crate::local_db::LocalDatabase;
use crate::sync::ignored_path;
use log::info;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

/// Periodically walks the user directory to discover assets on disk.
///
/// On each cycle the entire directory tree under `user_path` is scanned. Files that
/// are already tracked in the local database are skipped. New files are hashed and
/// inserted without an Immich asset ID. The upload worker later picks up these unlinked
/// records and syncs them to the server.
///
/// This complements the file watcher: the watcher reacts to live filesystem events,
/// while this worker catches anything that was already present before startup or that
/// the watcher may have missed.
pub async fn discovery_worker(
    cancel: CancellationToken,
    local_db: Arc<Mutex<LocalDatabase>>,
    user_path: PathBuf,
    user_id: String,
    poll_interval: u64,
    event_logger: Option<EventLogger>,
    exclude_extensions: Vec<String>,
) {
    info!("Discovery worker running...");

    loop {
        if cancel.is_cancelled() {
            return;
        }

        if let Some(el) = &event_logger {
            el.log(workers::DISCOVERY, "scan_started", &user_id, None, None, None);
        }

        for entry in WalkDir::new(&user_path).into_iter().filter_map(|e| e.ok()) {
            if cancel.is_cancelled() {
                return;
            }

            let path = entry.into_path();

            if ignored_path(&path, &exclude_extensions) {
                continue;
            }

            if !path.exists() {
                info!("File {} has disappeared, skipping", path.display());
                continue;
            }

            let relative_path = match path.strip_prefix(&user_path) {
                Ok(p) => p.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            if local_db.lock().await.find_asset_by_path(&user_id, &relative_path).ok().flatten().is_some() {
                continue;
            }

            let checksum = match hash_file(&path).await {
                Ok(c) => c,
                Err(e) => {
                    info!("Failed to hash {}: {}", path.display(), e);
                    continue;
                }
            };

            let created_at = match tokio::fs::metadata(&path).await {
                Ok(meta) => Some(super::uploader::file_created_at_string(&meta)),
                Err(_) => None,
            };

            if let Err(e) =
                local_db.lock().await.upsert_asset(&user_id, &relative_path, &checksum, None, created_at.as_deref())
            {
                info!("Failed to save asset: {}", e);
                continue;
            }

            if let Some(el) = &event_logger {
                el.log(workers::DISCOVERY, "file_discovered", &user_id, Some(&relative_path), None, None);
            }
        }

        if let Some(el) = &event_logger {
            el.log(workers::DISCOVERY, "scan_completed", &user_id, None, None, None);
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(poll_interval)) => {}
            _ = cancel.cancelled() => { return; }
        }
    }
}
