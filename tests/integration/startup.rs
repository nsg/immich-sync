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
async fn logs_server_version_and_v2_deprecation_warning() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    let version = api.server_version().await.expect("get server version");

    let (_guard, log_lines) = start_sync_service(&config_path).await;
    wait_for_log(&log_lines, "Immich server v").await;

    let logs = log_lines.lock().await;
    let deprecation_logged = logs.iter().any(|l| l.contains("Immich v2 support is deprecated"));
    if version.major == 2 {
        assert!(deprecation_logged, "Expected a v2 deprecation warning against Immich v{version}.\nLogs:\n{logs:?}");
    } else {
        assert!(!deprecation_logged, "Unexpected v2 deprecation warning against Immich v{version}.\nLogs:\n{logs:?}");
    }
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn discovery_finds_and_uploads_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    create_test_image(&user_dir, "test_discovery.jpg");
    let (_guard, _log_lines) = start_sync_service(&config_path).await;
    let _asset_id = wait_for_asset(&api, "test_discovery.jpg").await;

    // Verify the file was picked up by the discovery worker, not the file watcher
    let events = read_event_log(&el);
    assert!(
        !filter_events_by_worker_and_path(&events, "discovery", "test_discovery.jpg").is_empty(),
        "File should have been found by the discovery worker.\nEvents:\n{}",
        format_events(&events)
    );
    assert!(
        filter_events_by_worker_and_path(&events, "file_watcher", "test_discovery.jpg").is_empty(),
        "File should not have been detected by the file watcher.\nEvents:\n{}",
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn subdirectory_file_discovered_and_synced() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let sub_dir = user_dir.join("album2024");
    std::fs::create_dir_all(&sub_dir).expect("create subdirectory");

    // Create image in subdirectory before starting (discovery path)
    create_test_image(&sub_dir, "test_subdir.jpg");

    let (_guard, _log_lines) = start_sync_service(&config_path).await;
    let _asset_id = wait_for_asset(&api, "test_subdir.jpg").await;

    // Should be found by discovery, not inotify
    let events = read_event_log(&el);
    assert!(
        filter_events_by_worker_and_path(&events, "file_watcher", "test_subdir.jpg").is_empty(),
        "File should not have been detected by the file watcher.\nEvents:\n{}",
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn upload_dedup_links_existing_asset_without_reupload() {
    // Phase 1: Upload the file normally
    let (config_path1, _tmp1) = setup_config();
    let config1 = Config::load(config_path1.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config1.users[0].path);
    let api = ImmichAPI::new(&config1.immich.server_url, &config1.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    create_test_image(&user_dir, "test_dedup.jpg");
    let (_guard1, _log1) = start_sync_service(&config_path1).await;
    let _asset_id = wait_for_asset(&api, "test_dedup.jpg").await;

    // Phase 2: Kill service, wipe DB (but keep the image and the Immich asset)
    drop(_guard1);
    drop(_tmp1); // destroys tempdir including DB
    sleep(Duration::from_secs(1)).await;

    // Phase 3: Start fresh service with new DB — discovery finds the file,
    // bulk-upload-check returns "reject" (already on server) → links without uploading
    let (config_path2, _tmp2) = setup_config();
    let el = event_log_path(&_tmp2);
    let (_guard2, _log_lines2) = start_sync_service(&config_path2).await;

    // Wait for the bulk check to process this file
    wait_for_event(&el, "upload_check").await;

    // Give extra time for upload processing to complete
    sleep(Duration::from_secs(5)).await;

    // Should NOT have a file_uploaded event — dedup should have linked it
    let events = read_event_log(&el);
    assert!(
        filter_events_with_path(&events, "file_uploaded", "test_dedup.jpg").is_empty(),
        "File should have been deduped, not re-uploaded.\nEvents:\n{}",
        format_events(&events)
    );
    assert!(
        !filter_events_with_path(&events, "asset_linked", "test_dedup.jpg").is_empty(),
        "File should have been linked via dedup.\nEvents:\n{}",
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn multiple_files_uploaded_in_single_batch() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create 3 images with different content (different checksums) before starting
    create_test_image_with_suffix(&user_dir, "test_batch_a.jpg", b"aaa");
    create_test_image_with_suffix(&user_dir, "test_batch_b.jpg", b"bbb");
    create_test_image_with_suffix(&user_dir, "test_batch_c.jpg", b"ccc");

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Wait for all 3 to appear in Immich
    let _id_a = wait_for_asset(&api, "test_batch_a.jpg").await;
    let _id_b = wait_for_asset(&api, "test_batch_b.jpg").await;
    let _id_c = wait_for_asset(&api, "test_batch_c.jpg").await;

    // Verify all 3 were batched in one bulk check
    let events = read_event_log(&el);
    let check_events = filter_events(&events, "upload_check");
    assert!(
        check_events.iter().any(|e| e["detail"].as_str().is_some_and(|d| d.contains("3 assets"))),
        "All 3 files should be batched in one bulk-upload-check.\nEvents:\n{}",
        format_events(&events)
    );
}
