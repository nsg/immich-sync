use anyhow::Result;
use log::info;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::api::ImmichAPI;
use crate::config::Config;
use crate::event_log::EventLogger;
use crate::local_db::LocalDatabase;
use crate::workers::{deletion_watcher, discovery, file_watcher, uploader};

pub struct PathFilter {
    exclude_extensions: Vec<String>,
}

impl PathFilter {
    pub fn from_config(config: &Config) -> Self {
        Self { exclude_extensions: config.exclude_extensions.clone() }
    }

    /// `exclude_extensions` entries are lowercase without a leading dot
    /// (Config::load guarantees this).
    pub fn is_ignored(&self, path: &Path) -> bool {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with('.') {
                return true;
            }
        }
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            if self.exclude_extensions.iter().any(|e| ext.eq_ignore_ascii_case(e)) {
                return true;
            }
        }
        path.is_dir()
    }
}

/// Remove database entries for assets whose file extension matches an excluded extension.
/// This ensures that previously-tracked files are cleaned up when extensions are added
/// to the exclude list.
pub async fn purge_excluded_extensions(local_db: &Mutex<LocalDatabase>, config: &Config) {
    if config.exclude_extensions.is_empty() {
        return;
    }

    let filter = PathFilter::from_config(config);
    for user in &config.users {
        let db = local_db.lock().await;
        let unlinked = match db.find_unlinked_assets(&user.user_id) {
            Ok(v) => v,
            Err(e) => {
                info!("Failed to list assets for user {}: {}", user.user_id, e);
                continue;
            }
        };
        let mut count = 0;
        for (path, _) in unlinked {
            // Asset paths are relative to the user directory, so is_ignored's
            // path.is_dir() fallback never applies here; only the extension
            // (and dotfile) checks do.
            if !filter.is_ignored(Path::new(&path)) {
                continue;
            }
            match db.delete_asset(&user.user_id, &path) {
                Ok(()) => count += 1,
                Err(e) => info!("Failed to purge {} for user {}: {}", path, user.user_id, e),
            }
        }
        if count > 0 {
            info!("Purged {} assets with excluded extensions for user {}", count, user.user_id);
        }
    }
}

pub async fn run_user_sync(
    cancel: CancellationToken,
    local_db: Arc<Mutex<LocalDatabase>>,
    config: &Config,
    user_id: &str,
    event_logger: Option<EventLogger>,
    dry_run: bool,
) -> Result<()> {
    let user = config
        .users
        .iter()
        .find(|u| u.user_id == user_id)
        .unwrap_or_else(|| panic!("User {} not found in config", user_id));
    let api = Arc::new(Mutex::new(ImmichAPI::new(&config.immich.server_url, &user.user_key)));
    let user_path = Path::new(&user.path);

    info!("Starting sync for user {} at {}", user.user_id, user_path.display());

    if !user_path.exists() {
        anyhow::bail!("User path does not exist: {}", user_path.display());
    }

    let path_filter = Arc::new(PathFilter::from_config(config));

    let import_handle = tokio::spawn(discovery::discovery_worker(
        cancel.clone(),
        Arc::clone(&local_db),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.import_poll_interval,
        event_logger.clone(),
        Arc::clone(&path_filter),
    ));

    let upload_handle = tokio::spawn(uploader::upload_worker(
        cancel.clone(),
        Arc::clone(&local_db),
        Arc::clone(&api),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.upload_poll_interval,
        event_logger.clone(),
        dry_run,
    ));

    let file_handle = tokio::spawn(file_watcher::file_watcher(
        cancel.clone(),
        Arc::clone(&local_db),
        Arc::clone(&api),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.delete_threshold,
        config.immich.delete_max_age,
        event_logger.clone(),
        dry_run,
        Arc::clone(&path_filter),
    ));

    let deletion_handle = tokio::spawn(deletion_watcher::deletion_watcher(
        cancel.clone(),
        Arc::clone(&local_db),
        Arc::clone(&api),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.delete_poll_interval,
        config.immich.delete_max_age,
        event_logger,
        dry_run,
    ));

    tokio::select! {
        r = import_handle => {
            if let Err(e) = r {
                info!("Critical: Discovery worker task failed: {}", e);
            }
        }
        r = upload_handle => {
            if let Err(e) = r {
                info!("Critical: Upload worker task failed: {}", e);
            }
        }
        r = file_handle => {
            if let Err(e) = r {
                info!("Critical: File watcher task failed: {}", e);
            }
        }
        r = deletion_handle => {
            if let Err(e) = r {
                info!("Critical: Deletion watcher task failed: {}", e);
            }
        }
        _ = cancel.cancelled() => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_dotfiles() {
        let f = PathFilter { exclude_extensions: vec![] };
        assert!(f.is_ignored(Path::new("/data/.hidden")));
        assert!(f.is_ignored(Path::new("/data/.DS_Store")));
        assert!(f.is_ignored(Path::new(".gitignore")));
    }

    #[test]
    fn ignored_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("subdir");
        std::fs::create_dir(&dir).unwrap();
        let f = PathFilter { exclude_extensions: vec![] };
        assert!(f.is_ignored(&dir));
    }

    #[test]
    fn not_ignored_regular_files() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("photo.jpg");
        std::fs::write(&file, b"data").unwrap();
        let f = PathFilter { exclude_extensions: vec![] };
        assert!(!f.is_ignored(&file));
    }

    #[test]
    fn not_ignored_nested_file() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("album/photo.png");
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
        std::fs::write(&nested, b"data").unwrap();
        let f = PathFilter { exclude_extensions: vec![] };
        assert!(!f.is_ignored(&nested));
    }

    #[test]
    fn excluded_extension() {
        let f = PathFilter { exclude_extensions: vec!["mp4".to_string(), "mov".to_string()] };
        assert!(f.is_ignored(Path::new("video.mp4")));
        assert!(f.is_ignored(Path::new("video.mov")));
        assert!(!f.is_ignored(Path::new("photo.jpg")));
    }

    #[test]
    fn excluded_extension_file_case_insensitive() {
        let f = PathFilter { exclude_extensions: vec!["mp4".to_string(), "mov".to_string()] };
        assert!(f.is_ignored(Path::new("video.MP4")));
        assert!(f.is_ignored(Path::new("video.MOV")));
    }

    #[test]
    fn no_excludes_allows_all() {
        let f = PathFilter { exclude_extensions: vec![] };
        assert!(!f.is_ignored(Path::new("video.mp4")));
    }

    fn test_config(exclude_extensions: Vec<String>) -> Config {
        Config {
            database_path: String::new(),
            event_log: None,
            immich: crate::config::ImmichConfig {
                server_url: String::new(),
                delete_threshold: 0,
                delete_max_age: 3650,
                delete_poll_interval: 0,
                import_poll_interval: 0,
                upload_poll_interval: 60,
            },
            users: vec![crate::config::UserConfig {
                user_id: "user1".to_string(),
                user_key: String::new(),
                path: String::new(),
            }],
            exclude_extensions,
        }
    }

    fn purge_test_db(dir: &tempfile::TempDir) -> Mutex<LocalDatabase> {
        let db = LocalDatabase::open(&dir.path().join("test.db")).unwrap();
        db.upsert_asset("user1", "video.mp4", &[1u8; 20], None, None).unwrap();
        db.upsert_asset("user1", "keep.mp4", &[2u8; 20], Some("id-1"), None).unwrap();
        db.upsert_asset("user1", "photo.jpg", &[3u8; 20], None, None).unwrap();
        Mutex::new(db)
    }

    #[tokio::test]
    async fn purge_removes_only_unlinked_excluded_assets() {
        let dir = tempfile::tempdir().unwrap();
        let local_db = purge_test_db(&dir);
        let config = test_config(vec!["mp4".to_string()]);

        purge_excluded_extensions(&local_db, &config).await;

        let db = local_db.lock().await;
        assert!(db.find_asset_by_path("user1", "video.mp4").unwrap().is_none());
        assert!(db.find_asset_by_path("user1", "keep.mp4").unwrap().is_some());
        assert!(db.find_asset_by_path("user1", "photo.jpg").unwrap().is_some());
    }

    #[tokio::test]
    async fn purge_with_no_excludes_keeps_everything() {
        let dir = tempfile::tempdir().unwrap();
        let local_db = purge_test_db(&dir);
        let config = test_config(vec![]);

        purge_excluded_extensions(&local_db, &config).await;

        let db = local_db.lock().await;
        assert!(db.find_asset_by_path("user1", "video.mp4").unwrap().is_some());
        assert!(db.find_asset_by_path("user1", "keep.mp4").unwrap().is_some());
        assert!(db.find_asset_by_path("user1", "photo.jpg").unwrap().is_some());
    }
}
