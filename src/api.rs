use anyhow::{bail, Context, Result};
use log::error;
use reqwest::{header, multipart};
use serde::Deserialize;
use std::path::Path;

pub struct ImmichAPI {
    client: reqwest::Client,
    base_url: String,
}

#[derive(Deserialize)]
pub struct UploadResponse {
    pub id: Option<String>,
}

#[derive(Deserialize)]
pub struct BulkUploadCheckResponse {
    pub results: Vec<BulkCheckResult>,
}

#[derive(Deserialize)]
pub struct BulkCheckResult {
    pub id: String,
    pub action: String,
    #[serde(rename = "assetId")]
    pub asset_id: Option<String>,
}

pub struct BulkCheckInput {
    pub id: String,
    pub checksum_hex: String,
}

#[derive(Deserialize, Clone, Copy)]
pub struct ServerVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl std::fmt::Display for ServerVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl ImmichAPI {
    pub fn new(base_url: &str, api_key: &str) -> Self {
        let base = base_url.trim_end_matches('/');
        let base_url = if base.ends_with("/api") { base.to_string() } else { format!("{}/api", base) };

        let headers: header::HeaderMap = [
            (header::ACCEPT, "application/json".parse().unwrap()),
            ("x-api-key".parse().unwrap(), api_key.parse().unwrap()),
        ]
        .into_iter()
        .collect();

        let client = reqwest::Client::builder().default_headers(headers).build().expect("Failed to build HTTP client");

        Self { client, base_url }
    }

    fn url(&self, path: &str) -> String {
        let path = path.trim_matches('/');
        assert!(!path.is_empty(), "API path must not be empty");
        format!("{}/{}", self.base_url, path)
    }

    async fn delete_json(&self, path: &str, body: &serde_json::Value) -> Result<reqwest::Response> {
        self.client.delete(self.url(path)).json(body).send().await.with_context(|| format!("DELETE {} failed", path))
    }

    pub async fn upload_asset(
        &self,
        path: &Path,
        device_asset_id: &str,
        file_created_at: &str,
        file_modified_at: &str,
    ) -> Result<Option<UploadResponse>> {
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .with_context(|| format!("Invalid file name: {}", path.display()))?
            .to_string();

        let file_bytes =
            tokio::fs::read(path).await.with_context(|| format!("Failed to read file {}", path.display()))?;

        let file_part = multipart::Part::bytes(file_bytes).file_name(file_name);

        let form = multipart::Form::new()
            .text("deviceAssetId", device_asset_id.to_string())
            .text("deviceId", "sync-service")
            .text("fileCreatedAt", file_created_at.to_string())
            .text("fileModifiedAt", file_modified_at.to_string())
            .part("assetData", file_part);

        let resp =
            self.client.post(self.url("assets")).multipart(form).send().await.context("Failed to upload asset")?;

        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();

        if status != 200 && status != 201 {
            error!("Failed to upload asset {}, status code {}. Response: {}", path.display(), status, body);
            return Ok(None);
        }

        let upload_resp: UploadResponse =
            serde_json::from_str(&body).with_context(|| format!("Failed to parse upload response: {}", body))?;

        if upload_resp.id.is_none() {
            error!("Failed to upload asset {}, response: {}", path.display(), body);
        }

        Ok(Some(upload_resp))
    }

    #[allow(dead_code)] // used by integration tests via lib.rs
    pub async fn search_assets(&self, filename: &str) -> Result<Vec<serde_json::Value>> {
        let resp = self
            .client
            .post(self.url("search/metadata"))
            .json(&serde_json::json!({ "originalFileName": filename }))
            .send()
            .await
            .context("POST search/metadata failed")?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if status != 200 {
            bail!("POST search/metadata returned status {}. Response: {}", status, body);
        }
        let parsed: serde_json::Value =
            serde_json::from_str(&body).with_context(|| format!("Failed to parse search response: {}", body))?;
        let items = parsed["assets"]["items"].as_array().cloned().unwrap_or_default();
        Ok(items)
    }

    #[allow(dead_code)] // used by integration tests via lib.rs
    pub async fn empty_trash(&self) -> Result<()> {
        let resp = self.client.post(self.url("trash/empty")).send().await.context("POST trash/empty failed")?;
        let status = resp.status().as_u16();
        if status != 200 {
            let body = resp.text().await.unwrap_or_default();
            bail!("POST trash/empty returned status {}. Response: {}", status, body);
        }
        Ok(())
    }

    pub async fn delete_asset(&self, asset_id: &str) -> Result<()> {
        let resp = self.delete_json("assets", &serde_json::json!({"ids": [asset_id]})).await?;

        let status = resp.status().as_u16();
        if status != 204 {
            let body = resp.text().await.unwrap_or_default();
            bail!("Failed to delete asset {}, status code {}. Response: {}", asset_id, status, body);
        }

        Ok(())
    }

    pub async fn server_version(&self) -> Result<ServerVersion> {
        let resp = self.client.get(self.url("server/version")).send().await.context("GET server/version failed")?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if status != 200 {
            bail!("GET server/version returned status {}. Response: {}", status, body);
        }
        serde_json::from_str(&body).with_context(|| format!("Failed to parse server version response: {}", body))
    }

    /// Checks which assets already exist on the Immich server.
    pub async fn bulk_upload_check(&self, assets: &[BulkCheckInput]) -> Result<BulkUploadCheckResponse> {
        let payload: Vec<serde_json::Value> = assets
            .iter()
            .map(|a| {
                serde_json::json!({
                    "id": a.id,
                    "checksum": a.checksum_hex,
                })
            })
            .collect();

        let resp = self
            .client
            .post(self.url("assets/bulk-upload-check"))
            .json(&serde_json::json!({ "assets": payload }))
            .send()
            .await
            .context("bulk-upload-check request failed")?;

        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();

        if status != 200 {
            bail!("bulk-upload-check returned status {}. Response: {}", status, body);
        }

        serde_json::from_str(&body).with_context(|| format!("Failed to parse bulk-upload-check response: {}", body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_new_appends_api_path() {
        let api = ImmichAPI::new("http://localhost:3001", "test-key");
        assert_eq!(api.base_url, "http://localhost:3001/api");
    }

    #[test]
    fn api_new_preserves_existing_api_path() {
        let api = ImmichAPI::new("http://localhost:3001/api", "test-key");
        assert_eq!(api.base_url, "http://localhost:3001/api");
    }

    #[test]
    fn api_new_strips_trailing_slash() {
        let api = ImmichAPI::new("http://localhost:3001/", "test-key");
        assert_eq!(api.base_url, "http://localhost:3001/api");
    }

    #[test]
    #[should_panic]
    fn api_new_rejects_invalid_api_key() {
        ImmichAPI::new("http://localhost:3001", "bad\nkey");
    }

    #[test]
    fn url_bare_path() {
        let api = ImmichAPI::new("http://localhost:3001", "key");
        assert_eq!(api.url("users/me"), "http://localhost:3001/api/users/me");
    }

    #[test]
    fn url_leading_slash() {
        let api = ImmichAPI::new("http://localhost:3001", "key");
        assert_eq!(api.url("/users/me"), "http://localhost:3001/api/users/me");
    }

    #[test]
    fn url_trailing_slash() {
        let api = ImmichAPI::new("http://localhost:3001", "key");
        assert_eq!(api.url("users/me/"), "http://localhost:3001/api/users/me");
    }

    #[test]
    fn url_both_slashes() {
        let api = ImmichAPI::new("http://localhost:3001", "key");
        assert_eq!(api.url("/users/me/"), "http://localhost:3001/api/users/me");
    }

    #[test]
    #[should_panic(expected = "API path must not be empty")]
    fn url_empty_path() {
        let api = ImmichAPI::new("http://localhost:3001", "key");
        api.url("");
    }

    #[test]
    #[should_panic(expected = "API path must not be empty")]
    fn url_only_slashes() {
        let api = ImmichAPI::new("http://localhost:3001", "key");
        api.url("///");
    }
}
