use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

// ---------------------------------------------------------------------------
// Trait: abstracts S3 access so Store can be tested with an in-memory mock.
// ---------------------------------------------------------------------------

pub trait S3Access: Send + Sync + Clone + 'static {
    fn get_bytes<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'a;

    fn get_byte_range<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'a;

    fn get_body<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> impl std::future::Future<Output = Result<ByteStream>> + Send + 'a;

    fn put_bytes<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        body: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;

    fn put_file<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        path: &'a Path,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;

    /// Return all object keys under `prefix` with their sizes.
    fn list_keys<'a>(
        &'a self,
        bucket: &'a str,
        prefix: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<(String, u64)>>> + Send + 'a;

    /// Delete a single object.
    fn delete_object<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'a;
}

impl S3Access for Client {
    async fn get_bytes(&self, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let _timing = crate::metrics::timing!("s3.get");
        let resp = self
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("get_object failed for {key}"))?;

        Ok(resp
            .body
            .collect()
            .await
            .context("reading body")?
            .into_bytes()
            .to_vec())
    }

    async fn get_byte_range(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        let _timing = crate::metrics::timing!("s3.get_range");
        let range = format!("bytes={}-{}", offset, offset + len as u64 - 1);
        let resp = self
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .with_context(|| format!("range get_object failed for {key}"))?;

        Ok(resp
            .body
            .collect()
            .await
            .context("reading range body")?
            .into_bytes()
            .to_vec())
    }

    async fn get_body(&self, bucket: &str, key: &str) -> Result<ByteStream> {
        let _timing = crate::metrics::timing!("s3.get");
        let resp = self
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("get_object failed for {key}"))?;
        Ok(resp.body)
    }

    async fn put_bytes(&self, bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
        let _timing = crate::metrics::timing!("s3.put");
        self.put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .send()
            .await
            .with_context(|| format!("put_object failed for {key}"))?;
        Ok(())
    }

    async fn put_file(&self, bucket: &str, key: &str, path: &Path) -> Result<()> {
        let _timing = crate::metrics::timing!("s3.put");
        let body = ByteStream::from_path(path.to_path_buf())
            .await
            .with_context(|| format!("opening upload source {}", path.display()))?;
        self.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("put_object failed for {key}"))?;
        Ok(())
    }

    async fn list_keys(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, u64)>> {
        let _timing = crate::metrics::timing!("s3.list");
        let mut keys = Vec::new();
        let mut continuation = None;

        loop {
            let mut req = self.list_objects_v2().bucket(bucket).prefix(prefix);

            if let Some(token) = continuation {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.context("list_objects_v2 failed")?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let size = obj.size().unwrap_or(0) as u64;
                    keys.push((key.to_string(), size));
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(str::to_string);
            } else {
                break;
            }
        }

        Ok(keys)
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let _timing = crate::metrics::timing!("s3.delete");
        self.delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("delete_object failed for {key}"))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// S3 key helpers and state persistence
// ---------------------------------------------------------------------------

/// S3-existence index: which block indices live under a store prefix.
pub type BlockIndex = HashSet<u64>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub parent_id: Option<String>,
    pub block_size: u64,
    pub volume_size: u64,
    /// Non-empty means this store is frozen (no further writes allowed).
    pub children: Vec<String>,
}

pub fn normalize_prefix(prefix: &str) -> String {
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        "stores".to_string()
    } else {
        format!("{p}/stores")
    }
}

pub fn state_key(prefix: &str, store_id: &str) -> String {
    format!("{prefix}/{store_id}/state.json")
}

pub fn block_key(prefix: &str, store_id: &str, block_idx: u64) -> String {
    format!("{prefix}/{store_id}/{block_idx:016x}")
}

pub async fn read_state<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
    store_id: &str,
) -> Result<State> {
    let key = state_key(prefix, store_id);
    let bytes = s3
        .get_bytes(bucket, &key)
        .await
        .with_context(|| format!("reading state for {store_id}"))?;
    serde_json::from_slice(&bytes).context("parsing state.json")
}

pub async fn write_state<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
    store_id: &str,
    state: &State,
) -> Result<()> {
    let key = state_key(prefix, store_id);
    let body = serde_json::to_vec_pretty(state).context("serializing state")?;
    s3.put_bytes(bucket, &key, body)
        .await
        .with_context(|| format!("writing state for {store_id}"))
}

/// Returns `(block_index, tombstones)` where tombstones are block indices
/// whose S3 object has size == 0 (explicit zero-block markers).
pub async fn list_blocks<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
    store_id: &str,
) -> Result<(BlockIndex, HashSet<u64>)> {
    let store_prefix = format!("{prefix}/{store_id}/");
    let keys = s3
        .list_keys(bucket, &store_prefix)
        .await
        .context("list_keys failed")?;

    let mut index = BlockIndex::new();
    let mut tombstones = HashSet::new();
    for (key, size) in keys {
        let filename = key.trim_start_matches(&store_prefix);
        // Block files are plain hex (no dot). Skip state.json.
        if !filename.contains('.')
            && let Ok(idx) = u64::from_str_radix(filename, 16)
        {
            index.insert(idx);
            if size == 0 {
                tombstones.insert(idx);
            }
        }
    }
    Ok((index, tombstones))
}

pub async fn upload_block<S: S3Access>(
    s3: &S,
    bucket: &str,
    key: &str,
    path: &std::path::Path,
) -> Result<()> {
    if crate::assert::is_enabled() {
        let data = std::fs::read(path)
            .with_context(|| format!("reading {} for zero-block assertion", path.display()))?;
        loophole_assert!(
            !data.iter().all(|&b| b == 0),
            "about to upload an all-zero block to S3: key={key}, size={}",
            data.len()
        );
    }
    s3.put_file(bucket, key, path).await
}
