use anyhow::Result;
use chrono::{DateTime, Utc};
use log::info;
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use std::collections::HashSet;

use crate::api::{BulkCheckInput, ImmichAPI};
use crate::event_log::{workers, EventLogger};
use crate::hash::checksum_to_hex;
use crate::local_db::LocalDatabase;

const BATCH_SIZE: usize = 2000;

/// Periodically syncs locally-discovered assets to the Immich server.
///
/// The file watcher and discovery worker insert assets into the local database without
/// an Immich asset ID. This worker polls for those unlinked assets and reconciles them
/// against the server using the bulk-upload-check endpoint:
///
/// - **Already on server** — the local record is linked to the existing remote asset ID
///   without re-uploading.
/// - **Not on server** — the file is uploaded and the resulting asset ID is stored
///   locally.
///
/// Assets are processed in batches to avoid overwhelming the API.
#[allow(clippy::too_many_arguments)]
pub async fn upload_worker(
    cancel: CancellationToken,
    local_db: Arc<Mutex<LocalDatabase>>,
    api: Arc<Mutex<ImmichAPI>>,
    user_path: PathBuf,
    user_id: String,
    poll_interval: u64,
    event_logger: Option<EventLogger>,
    dry_run: bool,
) {
    info!("Upload worker running...");

    loop {
        if cancel.is_cancelled() {
            return;
        }

        let unknown_assets = match local_db.lock().await.find_unlinked_assets(&user_id) {
            Ok(a) => a,
            Err(e) => {
                info!("Failed to query unknown assets: {}", e);
                sleep(Duration::from_secs(poll_interval)).await;
                continue;
            }
        };

        if !unknown_assets.is_empty() {
            info!("Checking {} assets without Asset ID via bulk-upload-check", unknown_assets.len());
        }

        for chunk in unknown_assets.chunks(BATCH_SIZE) {
            if cancel.is_cancelled() {
                return;
            }

            let inputs: Vec<BulkCheckInput> = chunk
                .iter()
                .map(|(path, checksum)| BulkCheckInput { id: path.clone(), checksum_hex: checksum_to_hex(checksum) })
                .collect();

            if let Some(el) = &event_logger {
                el.log(
                    workers::UPLOADER,
                    "upload_check",
                    &user_id,
                    None,
                    None,
                    Some(&format!("{} assets", inputs.len())),
                );
            }

            let results = match api.lock().await.bulk_upload_check(&inputs).await {
                Ok(r) => r,
                Err(e) => {
                    info!("bulk-upload-check failed: {}", e);
                    break;
                }
            };

            let checksum_map: std::collections::HashMap<&str, &[u8]> =
                chunk.iter().map(|(path, checksum)| (path.as_str(), checksum.as_slice())).collect();

            let mut handled_checksums: HashSet<Vec<u8>> = HashSet::new();

            for result in &results.results {
                if cancel.is_cancelled() {
                    return;
                }

                if result.action == "reject" {
                    if let Some(asset_id) = &result.asset_id {
                        if let Some(checksum) = checksum_map.get(result.id.as_str()) {
                            handled_checksums.insert(checksum.to_vec());
                            if let Err(e) =
                                local_db.lock().await.link_asset_by_checksum(&user_id, checksum, asset_id, None)
                            {
                                info!("Failed to update asset ID: {}", e);
                            } else if let Some(el) = &event_logger {
                                el.log(
                                    workers::UPLOADER,
                                    "asset_linked",
                                    &user_id,
                                    Some(&result.id),
                                    Some(asset_id),
                                    None,
                                );
                            }
                        }
                    }
                } else {
                    let asset_path = user_path.join(&result.id);
                    if !asset_path.exists() {
                        info!("File {} has disappeared, removing from database", result.id);
                        if let Some(el) = &event_logger {
                            el.log(workers::UPLOADER, "file_disappeared", &user_id, Some(&result.id), None, None);
                        }
                        if let Err(e) = local_db.lock().await.delete_asset(&user_id, &result.id) {
                            info!("Failed to remove disappeared asset from database: {}", e);
                        }
                        continue;
                    }

                    if let Some(checksum) = checksum_map.get(result.id.as_str()) {
                        if !handled_checksums.insert(checksum.to_vec()) {
                            info!("{} deduplicated locally (same content already processed)", result.id);
                            if let Some(el) = &event_logger {
                                el.log(
                                    workers::UPLOADER,
                                    "upload_skipped_dedup",
                                    &user_id,
                                    Some(&result.id),
                                    None,
                                    None,
                                );
                            }
                            continue;
                        }
                        if dry_run {
                            if let Some(el) = &event_logger {
                                el.log(
                                    workers::UPLOADER,
                                    "upload_skipped",
                                    &user_id,
                                    Some(&result.id),
                                    None,
                                    Some("dry-run"),
                                );
                            }
                            continue;
                        }
                        info!("{} not found in Immich, uploading", result.id);
                        match import_asset(&local_db, &api, &user_id, &user_path, &asset_path, checksum).await {
                            Ok(Some(asset_id)) => {
                                if let Some(el) = &event_logger {
                                    el.log(
                                        workers::UPLOADER,
                                        "file_uploaded",
                                        &user_id,
                                        Some(&result.id),
                                        Some(&asset_id),
                                        None,
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                info!("Failed to upload {}: {}", result.id, e);
                                if let Some(el) = &event_logger {
                                    el.log(
                                        workers::UPLOADER,
                                        "upload_failed",
                                        &user_id,
                                        Some(&result.id),
                                        None,
                                        Some(&e.to_string()),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(poll_interval)) => {}
            _ = cancel.cancelled() => { return; }
        }
    }
}

async fn import_asset(
    local_db: &Arc<Mutex<LocalDatabase>>,
    api: &Arc<Mutex<ImmichAPI>>,
    user_id: &str,
    base_path: &Path,
    asset_path: &Path,
    checksum: &[u8],
) -> Result<Option<String>> {
    let relative_path = asset_path.strip_prefix(base_path)?.to_string_lossy().to_string();
    let metadata = tokio::fs::metadata(asset_path).await?;
    let info = asset_info(asset_path, &metadata);

    let upload_resp = api
        .lock()
        .await
        .upload_asset(asset_path, &info.device_asset_id, &info.file_created_at, &info.file_modified_at)
        .await?;

    if let Some(resp) = upload_resp {
        if let Some(id) = &resp.id {
            local_db.lock().await.link_asset_by_checksum(user_id, checksum, id, Some(&info.file_created_at))?;
            info!("Uploaded {} for user {} (asset_id: {})", relative_path, user_id, id);
            return Ok(Some(id.clone()));
        }
    }

    Ok(None)
}

struct AssetInfo {
    device_asset_id: String,
    file_created_at: String,
    file_modified_at: String,
}

fn system_time_to_secs(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs() as f64).unwrap_or(0.0)
}

fn asset_info(path: &Path, metadata: &Metadata) -> AssetInfo {
    let mtime = metadata.modified().map(system_time_to_secs).unwrap_or(0.0);
    let ctime = metadata.created().map(system_time_to_secs).unwrap_or(mtime);
    let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");
    AssetInfo {
        device_asset_id: format!("{}-{}", filename, mtime),
        file_created_at: timestamp_to_string(ctime),
        file_modified_at: timestamp_to_string(mtime),
    }
}

fn timestamp_to_string(ts: f64) -> String {
    DateTime::<Utc>::from_timestamp(ts as i64, 0).unwrap_or_default().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Returns the file's creation timestamp as an ISO-like string, using mtime as fallback.
pub(crate) fn file_created_at_string(metadata: &Metadata) -> String {
    let mtime = metadata.modified().map(system_time_to_secs).unwrap_or(0.0);
    let ctime = metadata.created().map(system_time_to_secs).unwrap_or(mtime);
    timestamp_to_string(ctime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_epoch_zero() {
        assert_eq!(timestamp_to_string(0.0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn timestamp_known_date() {
        // 2024-01-15 12:30:00 UTC = 1705321800
        assert_eq!(timestamp_to_string(1705321800.0), "2024-01-15T12:30:00Z");
    }

    #[test]
    fn timestamp_fractional_truncated() {
        assert_eq!(timestamp_to_string(0.999), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn asset_info_from_metadata() {
        use std::fs::FileTimes;
        use std::time::{Duration, SystemTime};

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("IMG_001.jpg");
        std::fs::write(&file_path, b"test").unwrap();

        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1705321800);
        let times = FileTimes::new().set_accessed(ts).set_modified(ts);
        std::fs::File::options().write(true).open(&file_path).unwrap().set_times(times).unwrap();

        let metadata = std::fs::metadata(&file_path).unwrap();
        let info = asset_info(&file_path, &metadata);

        assert_eq!(info.device_asset_id, "IMG_001.jpg-1705321800");
        assert_eq!(info.file_modified_at, "2024-01-15T12:30:00Z");
    }

    #[test]
    fn asset_info_no_filename() {
        use std::fs::FileTimes;
        use std::time::{Duration, SystemTime};

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("tmp");
        std::fs::write(&file_path, b"test").unwrap();

        let ts = SystemTime::UNIX_EPOCH + Duration::from_secs(1705321800);
        let times = FileTimes::new().set_accessed(ts).set_modified(ts);
        std::fs::File::options().write(true).open(&file_path).unwrap().set_times(times).unwrap();

        let metadata = std::fs::metadata(&file_path).unwrap();
        let info = asset_info(Path::new("/"), &metadata);

        assert_eq!(info.device_asset_id, "unknown-1705321800");
    }
}
