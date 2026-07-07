use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

pub struct CliArgs {
    pub config_path: String,
    pub dry_run: bool,
}

pub fn parse_cli_args() -> CliArgs {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path = "config.toml".to_string();
    let mut dry_run = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                if let Some(path) = args.get(i + 1) {
                    config_path = path.clone();
                    i += 1;
                } else {
                    eprintln!("Error: --config requires a path argument");
                    std::process::exit(1);
                }
            }
            "--dry-run" | "-n" => {
                dry_run = true;
            }
            _ => {}
        }
        i += 1;
    }
    CliArgs { config_path, dry_run }
}

#[derive(Deserialize)]
pub struct Config {
    pub database_path: String,
    pub event_log: Option<String>,
    pub immich: ImmichConfig,
    #[serde(rename = "user")]
    pub users: Vec<UserConfig>,
    #[serde(default)]
    pub exclude_extensions: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct ImmichConfig {
    pub server_url: String,
    pub delete_threshold: i64,
    pub delete_max_age: i64,
    pub delete_poll_interval: u64,
    pub import_poll_interval: u64,
    pub upload_poll_interval: u64,
}

#[derive(Deserialize, Clone)]
pub struct UserConfig {
    pub user_id: String,
    pub user_key: String,
    pub path: String,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path).with_context(|| format!("Failed to read config file: {}", path))?;
        let config: Config = toml::from_str(&content).with_context(|| "Failed to parse config file")?;
        Ok(config)
    }

    pub fn database_path(&self) -> PathBuf {
        PathBuf::from(&self.database_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn load_valid_config() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
database_path = "/data/sync-service.db"

[immich]
server_url = "http://localhost:2283"
delete_threshold = 365
delete_max_age = 3650
delete_poll_interval = 3600
import_poll_interval = 86400
upload_poll_interval = 60

[[user]]
user_id = "uuid-1"
user_key = "key-1"
path = "/data/photos/user1"

[[user]]
user_id = "uuid-2"
user_key = "key-2"
path = "/data/photos/user2"
"#
        )
        .unwrap();

        let config = Config::load(f.path().to_str().unwrap()).unwrap();
        assert_eq!(config.database_path, "/data/sync-service.db");
        assert_eq!(config.immich.server_url, "http://localhost:2283");
        assert_eq!(config.immich.delete_threshold, 365);
        assert_eq!(config.users.len(), 2);
        assert_eq!(config.users[0].user_id, "uuid-1");
        assert_eq!(config.users[0].user_key, "key-1");
        assert_eq!(config.users[0].path, "/data/photos/user1");
    }

    #[test]
    fn load_config_with_database_path() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
database_path = "/var/lib/sync/my-data.db"

[immich]
server_url = "http://localhost:2283"
delete_threshold = 365
delete_max_age = 3650
delete_poll_interval = 3600
import_poll_interval = 86400
upload_poll_interval = 60

[[user]]
user_id = "uuid-1"
user_key = "key-1"
path = "/data/photos/user1"
"#
        )
        .unwrap();

        let config = Config::load(f.path().to_str().unwrap()).unwrap();
        assert_eq!(config.database_path, "/var/lib/sync/my-data.db");
    }

    #[test]
    fn load_config_with_poll_interval() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
database_path = "/data/sync.db"

[immich]
server_url = "http://localhost:2283"
delete_threshold = 365
delete_max_age = 3650
delete_poll_interval = 7200
import_poll_interval = 86400
upload_poll_interval = 60

[[user]]
user_id = "uuid-1"
user_key = "key-1"
path = "/data/photos/user1"
"#
        )
        .unwrap();

        let config = Config::load(f.path().to_str().unwrap()).unwrap();
        assert_eq!(config.immich.delete_poll_interval, 7200);
    }

    #[test]
    fn load_config_with_exclude_extensions() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
database_path = "/data/sync.db"
exclude_extensions = ["mp4", "MOV", "avi"]

[immich]
server_url = "http://localhost:2283"
delete_threshold = 365
delete_max_age = 3650
delete_poll_interval = 3600
import_poll_interval = 86400
upload_poll_interval = 60

[[user]]
user_id = "uuid-1"
user_key = "key-1"
path = "/data/photos/user1"
"#
        )
        .unwrap();

        let config = Config::load(f.path().to_str().unwrap()).unwrap();
        assert_eq!(config.exclude_extensions, vec!["mp4", "MOV", "avi"]);
    }

    #[test]
    fn load_config_without_exclude_extensions_defaults_empty() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
database_path = "/data/sync.db"

[immich]
server_url = "http://localhost:2283"
delete_threshold = 365
delete_max_age = 3650
delete_poll_interval = 3600
import_poll_interval = 86400
upload_poll_interval = 60

[[user]]
user_id = "uuid-1"
user_key = "key-1"
path = "/data/photos/user1"
"#
        )
        .unwrap();

        let config = Config::load(f.path().to_str().unwrap()).unwrap();
        assert!(config.exclude_extensions.is_empty());
    }

    #[test]
    fn load_missing_file() {
        assert!(Config::load("/nonexistent/config.toml").is_err());
    }

    #[test]
    fn load_missing_field() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
[immich]
server_url = "localhost"
"#
        )
        .unwrap();

        assert!(Config::load(f.path().to_str().unwrap()).is_err());
    }

    #[test]
    fn database_path_returns_absolute() {
        let config = Config {
            database_path: "/etc/sync/my.db".to_string(),
            event_log: None,
            immich: ImmichConfig {
                server_url: String::new(),
                delete_threshold: 0,
                delete_max_age: 3650,
                delete_poll_interval: 0,
                import_poll_interval: 0,
                upload_poll_interval: 60,
            },
            users: vec![],
            exclude_extensions: vec![],
        };
        assert_eq!(config.database_path(), PathBuf::from("/etc/sync/my.db"));
    }

    #[test]
    fn load_missing_database_path() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
[immich]
server_url = "http://localhost:2283"
delete_threshold = 365

[[user]]
user_id = "uuid-1"
user_key = "key-1"
path = "/data/photos/user1"
"#
        )
        .unwrap();

        assert!(Config::load(f.path().to_str().unwrap()).is_err());
    }
}
