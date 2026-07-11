use anyhow::{Context, Result};
use log::info;
use rusqlite::Connection;
use std::path::Path;

pub struct LocalDatabase {
    conn: Connection,
}

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    asset_path TEXT NOT NULL,
    checksum BLOB NOT NULL,
    asset_id TEXT,
    created_at TEXT,
    UNIQUE(user_id, asset_path)
);
CREATE INDEX IF NOT EXISTS idx_assets_checksum ON assets(user_id, checksum);
CREATE INDEX IF NOT EXISTS idx_assets_asset_id ON assets(asset_id);
"#;

impl LocalDatabase {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).with_context(|| format!("Failed to open database: {}", path.display()))?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .context("Failed to set database pragmas")?;

        conn.execute_batch(SCHEMA_SQL).context("Failed to create database schema")?;

        info!("Database opened at {}", path.display());
        Ok(Self { conn })
    }

    pub fn upsert_asset(
        &self,
        user_id: &str,
        asset_path: &str,
        checksum: &[u8],
        asset_id: Option<&str>,
        created_at: Option<&str>,
    ) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO assets (user_id, asset_path, checksum, asset_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (user_id, asset_path) DO UPDATE SET
                     checksum = ?3,
                     asset_id = CASE WHEN assets.checksum != ?3 THEN ?4 ELSE COALESCE(?4, assets.asset_id) END,
                     created_at = COALESCE(?5, assets.created_at)",
                rusqlite::params![user_id, asset_path, checksum, asset_id, created_at],
            )
            .context("Failed to save asset")?;
        Ok(())
    }

    pub fn find_asset_by_path(&self, user_id: &str, asset_path: &str) -> Result<Option<AssetRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT asset_id FROM assets
             WHERE user_id = ?1 AND asset_path = ?2",
        )?;

        let result = stmt
            .query_row(rusqlite::params![user_id, asset_path], |row| Ok(AssetRow { asset_id: row.get(0)? }))
            .optional()?;

        Ok(result)
    }

    pub fn find_unlinked_assets(&self, user_id: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT asset_path, checksum FROM assets
             WHERE user_id = ?1 AND asset_id IS NULL",
        )?;

        let rows = stmt
            .query_map(rusqlite::params![user_id], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)))?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to query assets without ID")?;

        Ok(rows)
    }

    pub fn link_asset_by_checksum(
        &self,
        user_id: &str,
        checksum: &[u8],
        asset_id: &str,
        created_at: Option<&str>,
    ) -> Result<()> {
        self.conn
            .execute(
                "UPDATE assets SET asset_id = ?3, created_at = COALESCE(?4, created_at)
                 WHERE user_id = ?1 AND checksum = ?2 AND asset_id IS NULL",
                rusqlite::params![user_id, checksum, asset_id, created_at],
            )
            .context("Failed to update asset ID")?;
        Ok(())
    }

    pub fn delete_asset(&self, user_id: &str, asset_path: &str) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM assets WHERE user_id = ?1 AND asset_path = ?2",
                rusqlite::params![user_id, asset_path],
            )
            .context("Failed to remove asset")?;
        Ok(())
    }

    pub fn asset_age_days(&self, user_id: &str, asset_path: &str) -> Result<Option<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT CAST(julianday('now') - julianday(created_at) AS INTEGER)
             FROM assets
             WHERE user_id = ?1 AND asset_path = ?2 AND created_at IS NOT NULL",
        )?;

        let result = stmt.query_row(rusqlite::params![user_id, asset_path], |row| row.get::<_, i64>(0)).optional()?;

        Ok(result)
    }

    pub fn list_tracked_assets(&self, user_id: &str) -> Result<Vec<TrackedAsset>> {
        let mut stmt =
            self.conn.prepare("SELECT asset_path, checksum FROM assets WHERE user_id = ?1 AND asset_id IS NOT NULL")?;

        let rows = stmt
            .query_map(rusqlite::params![user_id], |row| {
                Ok(TrackedAsset { asset_path: row.get(0)?, checksum: row.get(1)? })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to query all assets")?;

        Ok(rows)
    }
}

pub struct AssetRow {
    pub asset_id: Option<String>,
}

pub struct TrackedAsset {
    pub asset_path: String,
    pub checksum: Vec<u8>,
}

trait OptionalExt<T> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalExt<T> for Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_db() -> (LocalDatabase, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let db = LocalDatabase::open(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn create_and_retrieve_asset() {
        let (db, _dir) = test_db();
        let checksum = vec![1u8; 20];
        db.upsert_asset("user1", "photos/test.jpg", &checksum, None, None).unwrap();

        let row = db.find_asset_by_path("user1", "photos/test.jpg").unwrap();
        assert!(row.is_some());
        assert!(row.unwrap().asset_id.is_none());
    }

    #[test]
    fn upsert_preserves_asset_id_when_checksum_unchanged() {
        let (db, _dir) = test_db();
        let checksum = vec![1u8; 20];
        db.upsert_asset("user1", "photos/test.jpg", &checksum, Some("uuid-123"), Some("2024-01-01T00:00:00Z")).unwrap();

        // Upsert with same checksum but no asset_id — should preserve existing
        db.upsert_asset("user1", "photos/test.jpg", &checksum, None, None).unwrap();

        let row = db.find_asset_by_path("user1", "photos/test.jpg").unwrap().unwrap();
        assert_eq!(row.asset_id.as_deref(), Some("uuid-123"));
    }

    #[test]
    fn upsert_clears_asset_id_when_checksum_changes() {
        let (db, _dir) = test_db();
        let checksum = vec![1u8; 20];
        db.upsert_asset("user1", "photos/test.jpg", &checksum, Some("uuid-123"), Some("2024-01-01T00:00:00Z")).unwrap();

        // Upsert with new checksum — asset_id should be cleared for re-upload
        let new_checksum = vec![2u8; 20];
        db.upsert_asset("user1", "photos/test.jpg", &new_checksum, None, None).unwrap();

        let row = db.find_asset_by_path("user1", "photos/test.jpg").unwrap().unwrap();
        assert!(row.asset_id.is_none(), "asset_id should be cleared when checksum changes");
    }

    #[test]
    fn get_assets_without_id() {
        let (db, _dir) = test_db();
        db.upsert_asset("user1", "a.jpg", &[1u8; 20], None, None).unwrap();
        db.upsert_asset("user1", "b.jpg", &[2u8; 20], Some("uuid-1"), None).unwrap();
        db.upsert_asset("user1", "c.jpg", &[3u8; 20], None, None).unwrap();

        let missing = db.find_unlinked_assets("user1").unwrap();
        assert_eq!(missing.len(), 2);
        let paths: Vec<&str> = missing.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"a.jpg"));
        assert!(paths.contains(&"c.jpg"));
    }

    #[test]
    fn update_asset_id_by_checksum() {
        let (db, _dir) = test_db();
        let checksum = vec![1u8; 20];
        db.upsert_asset("user1", "test.jpg", &checksum, None, None).unwrap();

        db.link_asset_by_checksum("user1", &checksum, "uuid-456", Some("2024-06-15T12:00:00Z")).unwrap();

        let row = db.find_asset_by_path("user1", "test.jpg").unwrap().unwrap();
        assert_eq!(row.asset_id.as_deref(), Some("uuid-456"));
    }

    #[test]
    fn remove_asset() {
        let (db, _dir) = test_db();
        db.upsert_asset("user1", "test.jpg", &[1u8; 20], None, None).unwrap();
        db.delete_asset("user1", "test.jpg").unwrap();
        assert!(db.find_asset_by_path("user1", "test.jpg").unwrap().is_none());
    }

    #[test]
    fn get_all_assets() {
        let (db, _dir) = test_db();
        db.upsert_asset("user1", "a.jpg", &[1u8; 20], Some("id-a"), None).unwrap();
        db.upsert_asset("user1", "b.jpg", &[2u8; 20], None, None).unwrap();
        db.upsert_asset("user2", "c.jpg", &[3u8; 20], Some("id-c"), None).unwrap();

        // Only returns assets with an asset_id (i.e. uploaded to Immich)
        let assets = db.list_tracked_assets("user1").unwrap();
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].asset_path, "a.jpg");
    }

    #[test]
    fn user_isolation() {
        let (db, _dir) = test_db();
        db.upsert_asset("user1", "photo.jpg", &[1u8; 20], Some("id-1"), None).unwrap();
        db.upsert_asset("user2", "photo.jpg", &[2u8; 20], Some("id-2"), None).unwrap();

        let row1 = db.find_asset_by_path("user1", "photo.jpg").unwrap().unwrap();
        let row2 = db.find_asset_by_path("user2", "photo.jpg").unwrap().unwrap();
        assert_eq!(row1.asset_id.as_deref(), Some("id-1"));
        assert_eq!(row2.asset_id.as_deref(), Some("id-2"));
    }
}
