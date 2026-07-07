use std::path::PathBuf;
use std::time::Duration;

use serial_test::serial;
use sync_service::api::ImmichAPI;
use sync_service::config::Config;
use tokio::time::sleep;

use crate::common::*;

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn duplicate_content_discovered_and_linked() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create two files with identical content (same suffix → same SHA1) before service start
    let shared_suffix = b"shared_dedup_content";
    create_test_image_with_suffix(&user_dir, "dedup_a.jpg", shared_suffix);
    create_test_image_with_suffix(&user_dir, "dedup_b.jpg", shared_suffix);

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Wait for bulk check to process both
    wait_for_event(&el, "upload_check").await;

    // Wait for the one upload to complete and both files to be linked to the asset
    wait_for_event(&el, "file_uploaded").await;
    wait_for_assets_linked(&config.database_path(), &config.users[0].user_id, &["dedup_a.jpg", "dedup_b.jpg"]).await;

    // Give extra time for upload processing to complete
    sleep(Duration::from_secs(5)).await;

    // At most one file_uploaded for the dedup files — the second should be deduped
    let events = read_event_log(&el);
    let upload_count = events
        .iter()
        .filter(|e| {
            e["event"].as_str() == Some("file_uploaded")
                && e["path"].as_str().is_some_and(|p| p.contains("dedup_a.jpg") || p.contains("dedup_b.jpg"))
        })
        .count();
    assert!(
        upload_count <= 1,
        "Expected at most 1 upload for identical-content files, got {}.\nEvents:\n{}",
        upload_count,
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn duplicate_content_inotify_and_linked() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start service FIRST, then create files (inotify path)
    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    let shared_suffix = b"shared_inotify_dedup";
    create_test_image_with_suffix(&user_dir, "inotify_dup_a.jpg", shared_suffix);
    create_test_image_with_suffix(&user_dir, "inotify_dup_b.jpg", shared_suffix);

    // Both should be detected by inotify
    wait_for_event_with_path(&el, "file_detected", "inotify_dup_a.jpg").await;
    wait_for_event_with_path(&el, "file_detected", "inotify_dup_b.jpg").await;

    // Wait for the one upload to complete and both files to be linked to the asset
    wait_for_event(&el, "file_uploaded").await;
    wait_for_assets_linked(
        &config.database_path(),
        &config.users[0].user_id,
        &["inotify_dup_a.jpg", "inotify_dup_b.jpg"],
    )
    .await;

    // Give time for upload processing to complete
    sleep(Duration::from_secs(5)).await;

    // At most one actual upload — the second should be deduped by bulk-upload-check
    let events = read_event_log(&el);
    let upload_count = events
        .iter()
        .filter(|e| {
            e["event"].as_str() == Some("file_uploaded")
                && e["path"]
                    .as_str()
                    .is_some_and(|p| p.contains("inotify_dup_a.jpg") || p.contains("inotify_dup_b.jpg"))
        })
        .count();
    assert!(
        upload_count <= 1,
        "Expected at most 1 upload for identical-content files, got {}.\nEvents:\n{}",
        upload_count,
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn duplicate_content_across_subdirectories() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create identical files in different subdirectories before service start
    let album1 = user_dir.join("album1");
    let album2 = user_dir.join("album2");
    std::fs::create_dir_all(&album1).expect("create album1");
    std::fs::create_dir_all(&album2).expect("create album2");

    let shared_suffix = b"shared_subdir_dedup";
    create_test_image_with_suffix(&album1, "photo.jpg", shared_suffix);
    create_test_image_with_suffix(&album2, "photo.jpg", shared_suffix);

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Wait for bulk check and upload processing
    wait_for_event(&el, "upload_check").await;
    wait_for_event(&el, "file_uploaded").await;
    wait_for_assets_linked(
        &config.database_path(),
        &config.users[0].user_id,
        &["album1/photo.jpg", "album2/photo.jpg"],
    )
    .await;

    // Give extra time for processing
    sleep(Duration::from_secs(5)).await;

    // Verify at most one upload
    let events = read_event_log(&el);
    let upload_count = events
        .iter()
        .filter(|e| {
            e["event"].as_str() == Some("file_uploaded") && e["path"].as_str().is_some_and(|p| p.contains("photo.jpg"))
        })
        .count();
    assert!(
        upload_count <= 1,
        "Expected at most 1 upload for identical-content files across subdirs, got {}.\nEvents:\n{}",
        upload_count,
        format_events(&events)
    );
}
