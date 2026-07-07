use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sync_service::api::ImmichAPI;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Minimal valid JPEG (same as the base64 blob in the old bash test).
pub const TEST_JPEG: &[u8] = &[
    0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
    0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x03, 0x02, 0x02, 0x03, 0x02, 0x02, 0x03, 0x03, 0x03, 0x03, 0x04, 0x03, 0x03,
    0x04, 0x05, 0x08, 0x05, 0x05, 0x04, 0x04, 0x05, 0x0A, 0x07, 0x07, 0x06, 0x08, 0x0C, 0x0A, 0x0C, 0x0C, 0x0B, 0x0A,
    0x0B, 0x0B, 0x0D, 0x0E, 0x12, 0x10, 0x0D, 0x0E, 0x11, 0x0E, 0x0B, 0x0B, 0x10, 0x16, 0x10, 0x11, 0x13, 0x14, 0x15,
    0x15, 0x15, 0x0C, 0x0F, 0x17, 0x18, 0x16, 0x14, 0x18, 0x12, 0x14, 0x15, 0x14, 0xFF, 0xDB, 0x00, 0x43, 0x01, 0x03,
    0x04, 0x04, 0x05, 0x04, 0x05, 0x09, 0x05, 0x05, 0x09, 0x14, 0x0D, 0x0B, 0x0D, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14,
    0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14,
    0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0x14,
    0x14, 0x14, 0x14, 0x14, 0x14, 0x14, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01, 0x11, 0x00,
    0xFF, 0xC4, 0x00, 0x14, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x09, 0xFF, 0xC4, 0x00, 0x14, 0x10, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xC4, 0x00, 0x14, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xFF, 0xC4, 0x00, 0x14, 0x11, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00,
    0x00, 0x3F, 0x00, 0xB0, 0x00, 0x1F, 0xFF, 0xD9,
];

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"))
}

pub struct ConfigOverrides {
    pub delete_threshold: i64,
    pub delete_max_age: i64,
}

impl Default for ConfigOverrides {
    fn default() -> Self {
        Self { delete_threshold: 365, delete_max_age: 3650 }
    }
}

pub fn setup_config() -> (PathBuf, tempfile::TempDir) {
    setup_config_with_overrides(&ConfigOverrides::default())
}

pub fn setup_config_with_overrides(overrides: &ConfigOverrides) -> (PathBuf, tempfile::TempDir) {
    let user_id = required_env("INTEGRATION_USER_ID");
    let server_url = required_env("INTEGRATION_SERVER_URL");
    let user_path = required_env("INTEGRATION_USER_PATH");
    let tmp = tempfile::tempdir().expect("failed to create tempdir");
    let db_path = tmp.path().join("sync-service.db");
    let config_path = tmp.path().join("config.toml");
    let api_key = required_env("INTEGRATION_API_KEY");
    let event_log = tmp.path().join("events.jsonl");

    let db = db_path.display();
    let el = event_log.display();
    let lines = vec![
        format!("database_path = \"{db}\""),
        format!("event_log = \"{el}\""),
        String::new(),
        "[immich]".into(),
        format!("server_url = \"{server_url}\""),
        format!("delete_threshold = {}", overrides.delete_threshold),
        format!("delete_max_age = {}", overrides.delete_max_age),
        "delete_poll_interval = 5".into(),
        "import_poll_interval = 86400".into(),
        "upload_poll_interval = 5".into(),
        String::new(),
        "[[user]]".into(),
        format!("user_id = \"{user_id}\""),
        format!("user_key = \"{api_key}\""),
        format!("path = \"{user_path}\""),
    ];
    std::fs::write(&config_path, lines.join("\n")).expect("write config");

    (config_path, tmp)
}

/// Guard that kills the child process on drop (panic or normal exit).
pub struct ChildGuard(tokio::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

pub fn create_test_image(user_dir: &std::path::Path, name: &str) -> PathBuf {
    create_test_image_with_suffix(user_dir, name, name.as_bytes())
}

pub fn create_test_image_with_suffix(user_dir: &std::path::Path, name: &str, suffix: &[u8]) -> PathBuf {
    let image_path = user_dir.join(name);
    {
        let mut f = std::fs::File::create(&image_path).expect("create image");
        f.write_all(TEST_JPEG).expect("write image");
        if !suffix.is_empty() {
            f.write_all(suffix).expect("write suffix");
        }
    }
    assert!(image_path.exists(), "test image was not created");
    image_path
}

pub type LogLines = Arc<Mutex<Vec<String>>>;

pub async fn start_sync_service(config_path: &PathBuf) -> (ChildGuard, LogLines) {
    start_sync_service_with_args(config_path, &[]).await
}

#[allow(dead_code)]
pub async fn start_sync_service_dry_run(config_path: &PathBuf) -> (ChildGuard, LogLines) {
    start_sync_service_with_args(config_path, &["--dry-run"]).await
}

async fn start_sync_service_with_args(config_path: &PathBuf, extra_args: &[&str]) -> (ChildGuard, LogLines) {
    let bin = env!("CARGO_BIN_EXE_sync-service");
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--config").arg(config_path);
    for arg in extra_args {
        cmd.arg(arg);
    }
    let mut child = cmd
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to start sync-service");
    let stderr = child.stderr.take().expect("stderr not captured");
    let guard = ChildGuard(child);

    // Collect log lines in the background
    let log_lines: LogLines = Arc::new(Mutex::new(Vec::new()));
    let log_lines_bg = log_lines.clone();
    let mut lines = BufReader::new(stderr).lines();
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            log_lines_bg.lock().await.push(line);
        }
    });

    // Give all workers time to fully initialize
    sleep(Duration::from_secs(5)).await;

    (guard, log_lines)
}

/// Delete all assets in Immich and clear the local user directory.
pub async fn clean_slate(api: &ImmichAPI, user_dir: &std::path::Path) {
    // Delete all remote assets
    loop {
        let assets = api.search_assets("").await.expect("search assets during cleanup");
        if assets.is_empty() {
            break;
        }
        let ids: Vec<&str> = assets.iter().filter_map(|a| a["id"].as_str()).collect();
        for id in &ids {
            api.delete_asset(id).await.expect("delete asset during cleanup");
        }
        api.empty_trash().await.expect("empty trash during cleanup");
        sleep(Duration::from_secs(1)).await;
    }

    // Clear local files and recreate directory
    if user_dir.exists() {
        std::fs::remove_dir_all(user_dir).expect("clean user dir");
    }
    std::fs::create_dir_all(user_dir).expect("create user dir");
}

pub async fn wait_for_asset(api: &ImmichAPI, filename: &str) -> String {
    for _ in 1..=60 {
        if let Ok(assets) = api.search_assets(filename).await {
            if let Some(id) = assets.first().and_then(|a| a["id"].as_str()) {
                return id.to_string();
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!("Asset '{filename}' did not appear in Immich within 60s");
}

#[allow(dead_code)]
pub async fn wait_for_no_asset(api: &ImmichAPI, filename: &str) {
    for _ in 1..=60 {
        if let Ok(assets) = api.search_assets(filename).await {
            if assets.is_empty() {
                return;
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!("Asset '{filename}' still present in Immich after 60s");
}

#[allow(dead_code)]
pub async fn wait_for_log(log_lines: &LogLines, substring: &str) {
    for _ in 1..=60 {
        let logs = log_lines.lock().await;
        if logs.iter().any(|l| l.contains(substring)) {
            return;
        }
        drop(logs);
        sleep(Duration::from_secs(1)).await;
    }
    panic!("Log line containing '{substring}' did not appear within 60s");
}

pub async fn wait_for_file_removed(path: &std::path::Path) {
    for _ in 1..=120 {
        if !path.exists() {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!("Local file {} was not removed within 120s", path.display());
}

// ── Event-log helpers ──────────────────────────────────────────────

pub fn event_log_path(tmp: &tempfile::TempDir) -> PathBuf {
    tmp.path().join("events.jsonl")
}

pub fn read_event_log(path: &Path) -> Vec<serde_json::Value> {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    content
        .lines()
        .enumerate()
        .map(|(idx, line)| {
            serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("Malformed JSON in event log {} at line {}: {}\nline: {}", path.display(), idx + 1, e, line)
            })
        })
        .collect()
}

pub fn filter_events<'a>(events: &'a [serde_json::Value], event_type: &str) -> Vec<&'a serde_json::Value> {
    events.iter().filter(|e| e["event"].as_str() == Some(event_type)).collect()
}

pub fn filter_events_with_path<'a>(
    events: &'a [serde_json::Value],
    event_type: &str,
    path_contains: &str,
) -> Vec<&'a serde_json::Value> {
    events
        .iter()
        .filter(|e| {
            e["event"].as_str() == Some(event_type) && e["path"].as_str().is_some_and(|p| p.contains(path_contains))
        })
        .collect()
}

pub fn filter_events_by_worker_and_path<'a>(
    events: &'a [serde_json::Value],
    worker: &str,
    path_contains: &str,
) -> Vec<&'a serde_json::Value> {
    events
        .iter()
        .filter(|e| {
            e["worker"].as_str() == Some(worker) && e["path"].as_str().is_some_and(|p| p.contains(path_contains))
        })
        .collect()
}

pub fn format_events(events: &[serde_json::Value]) -> String {
    events.iter().map(|e| serde_json::to_string(e).unwrap_or_default()).collect::<Vec<_>>().join("\n")
}

pub async fn wait_for_event(event_log: &Path, event_type: &str) {
    for _ in 1..=60 {
        let events = read_event_log(event_log);
        if !filter_events(&events, event_type).is_empty() {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let events = read_event_log(event_log);
    panic!("Event '{event_type}' did not appear within 60s.\nAll events:\n{}", format_events(&events));
}

pub async fn wait_for_event_with_path(event_log: &Path, event_type: &str, path_contains: &str) {
    for _ in 1..=60 {
        let events = read_event_log(event_log);
        if !filter_events_with_path(&events, event_type, path_contains).is_empty() {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let events = read_event_log(event_log);
    panic!(
        "Event '{event_type}' with path containing '{path_contains}' did not appear within 60s.\nAll events:\n{}",
        format_events(&events)
    );
}

#[allow(dead_code)]
pub async fn wait_for_n_events(event_log: &Path, event_type: &str, n: usize) {
    for _ in 1..=60 {
        let events = read_event_log(event_log);
        if filter_events(&events, event_type).len() >= n {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let events = read_event_log(event_log);
    let found = filter_events(&events, event_type).len();
    panic!("Expected {n} '{event_type}' events, found {found} within 60s.\nAll events:\n{}", format_events(&events));
}

#[allow(dead_code)]
pub async fn wait_for_n_events_with_path(event_log: &Path, event_type: &str, path_contains: &str, n: usize) {
    for _ in 1..=60 {
        let events = read_event_log(event_log);
        if filter_events_with_path(&events, event_type, path_contains).len() >= n {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    let events = read_event_log(event_log);
    let found = filter_events_with_path(&events, event_type, path_contains).len();
    panic!(
        "Expected {n} '{event_type}' events with path containing '{path_contains}', found {found} within 60s.\nAll events:\n{}",
        format_events(&events)
    );
}

#[allow(dead_code)]
pub fn filter_events_with_detail<'a>(
    events: &'a [serde_json::Value],
    event_type: &str,
    detail: &str,
) -> Vec<&'a serde_json::Value> {
    events.iter().filter(|e| e["event"].as_str() == Some(event_type) && e["detail"].as_str() == Some(detail)).collect()
}

/// Wait until all given asset paths are linked to an Immich asset ID in the
/// service's local database. Dedup of identical-content files can happen in
/// several timing-dependent ways (same-batch skip, checksum link after upload,
/// bulk-check reject), so the linked end state is the only reliable signal.
#[allow(dead_code)]
pub async fn wait_for_assets_linked(db_path: &Path, user_id: &str, paths: &[&str]) {
    for _ in 1..=60 {
        let all_linked = rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map(|conn| {
                paths.iter().all(|p| {
                    conn.query_row(
                        "SELECT asset_id IS NOT NULL FROM assets WHERE user_id = ?1 AND asset_path = ?2",
                        rusqlite::params![user_id, p],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if all_linked {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!("Assets {paths:?} were not all linked to an Immich asset ID within 60s");
}

/// Directly modify the sync-service's SQLite DB to set a custom created_at timestamp.
#[allow(dead_code)]
pub fn set_asset_created_at(db_path: &Path, asset_path: &str, created_at: &str) {
    let conn = rusqlite::Connection::open(db_path).expect("open DB for created_at override");
    let rows_affected = conn
        .execute("UPDATE assets SET created_at = ?1 WHERE asset_path = ?2", rusqlite::params![created_at, asset_path])
        .expect("update created_at");
    assert!(rows_affected > 0, "No rows updated for asset_path '{asset_path}' — is it in the DB yet?");
}
