mod api;
mod config;
mod event_log;
mod hash;
mod local_db;
mod policy;
mod sync;
mod workers;

use anyhow::Result;
use api::ImmichAPI;
use config::{parse_cli_args, Config};
use event_log::EventLogger;
use local_db::LocalDatabase;
use log::{info, warn};
use std::sync::Arc;
use sync::run_user_sync;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).format_timestamp(None).init();

    let cli = parse_cli_args();
    let config = Config::load(&cli.config_path)?;

    if cli.dry_run {
        info!("Dry-run mode enabled — no changes will be made to Immich or disk");
    }

    let event_logger = config.event_log.as_deref().map(EventLogger::open).transpose()?;

    let config = Arc::new(config);

    let db_path = config.database_path();
    let local_db = LocalDatabase::open(&db_path)?;
    let local_db = Arc::new(Mutex::new(local_db));

    // Purge any previously-tracked assets that now match excluded extensions
    sync::purge_excluded_extensions(&local_db, &config).await;

    if config.users.is_empty() {
        info!("No users configured, exiting");
        return Ok(());
    }

    check_server_version(&config).await;

    let cancel = CancellationToken::new();

    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to register signal handler");
        info!("Received shutdown signal, shutting down...");
        cancel_for_signal.cancel();
    });

    let mut handles = Vec::new();
    let user_ids: Vec<String> = config.users.iter().map(|u| u.user_id.clone()).collect();
    for user_id in user_ids {
        let cancel = cancel.clone();
        let local_db = Arc::clone(&local_db);
        let config = Arc::clone(&config);
        let event_logger = event_logger.clone();

        let dry_run = cli.dry_run;
        let handle = tokio::spawn(async move {
            if let Err(e) = run_user_sync(cancel, local_db, &config, &user_id, event_logger, dry_run).await {
                info!("User sync task failed: {}", e);
            }
        });
        handles.push(handle);
    }

    loop {
        tokio::select! {
            _ = sleep(Duration::from_secs(3600)) => {
                for (i, handle) in handles.iter().enumerate() {
                    if handle.is_finished() {
                        info!("Critical: User sync task {} has finished unexpectedly", i);
                    }
                }
            }
            _ = cancel.cancelled() => {
                break;
            }
        }
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn check_server_version(config: &Config) {
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    match api.server_version().await {
        Ok(version) if version.major == 2 => {
            warn!(
                "Immich server v{} detected. Immich v2 support is deprecated \
                 and will be removed in 0.3.x releases — please upgrade to Immich v3",
                version
            );
        }
        Ok(version) => info!("Immich server v{} detected", version),
        Err(e) => warn!("Could not determine Immich server version: {:#}", e),
    }
}
