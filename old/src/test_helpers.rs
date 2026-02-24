//! Shared test infrastructure: MockS3 and helpers used by store_tests and
//! volume_manager_tests.

use crate::s3::S3Access;
use crate::store::{State, Store};
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

// -----------------------------------------------------------------------
// Global cache – one per test-binary process
// -----------------------------------------------------------------------

static CACHE_INIT: OnceCell<()> = OnceCell::const_new();

pub async fn ensure_global_cache() {
    CACHE_INIT
        .get_or_init(|| async {
            let dir = std::env::temp_dir().join(format!("loophole_test_{}", std::process::id()));
            crate::cache::init(dir, 512 * 1024 * 1024)
                .await
                .expect("failed to init test cache");
        })
        .await;
}

static ID_SEQ: AtomicU64 = AtomicU64::new(0);
pub fn unique_id(tag: &str) -> String {
    let n = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{tag}-{n}")
}

// -----------------------------------------------------------------------
// In-memory mock S3
// -----------------------------------------------------------------------

#[derive(Clone)]
pub struct MockS3 {
    objects: Arc<std::sync::RwLock<HashMap<String, Vec<u8>>>>,
    get_body_calls: Arc<AtomicU64>,
    put_file_calls: Arc<AtomicU64>,
    get_body_delay_ms: Arc<AtomicU64>,
    pub put_file_fail: Arc<AtomicBool>,
}

impl MockS3 {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(std::sync::RwLock::new(HashMap::new())),
            get_body_calls: Arc::new(AtomicU64::new(0)),
            put_file_calls: Arc::new(AtomicU64::new(0)),
            get_body_delay_ms: Arc::new(AtomicU64::new(0)),
            put_file_fail: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn put(&self, key: &str, data: Vec<u8>) {
        self.objects.write().unwrap().insert(key.to_string(), data);
    }

    pub fn set_get_body_delay_ms(&self, ms: u64) {
        self.get_body_delay_ms.store(ms, Ordering::Relaxed);
    }

    pub fn get_body_calls(&self) -> u64 {
        self.get_body_calls.load(Ordering::Relaxed)
    }

    pub fn put_file_calls(&self) -> u64 {
        self.put_file_calls.load(Ordering::Relaxed)
    }

    pub fn get_object(&self, key: &str) -> Option<Vec<u8>> {
        self.objects.read().unwrap().get(key).cloned()
    }
}

impl S3Access for MockS3 {
    async fn get_bytes(&self, _bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("MockS3: not found: {key}"))
    }

    async fn get_byte_range(
        &self,
        _bucket: &str,
        key: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>> {
        let data = self
            .objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("MockS3: not found: {key}"))?;
        let start = offset as usize;
        let end = (start + len).min(data.len());
        Ok(data[start..end].to_vec())
    }

    async fn get_body(&self, _bucket: &str, key: &str) -> Result<ByteStream> {
        self.get_body_calls.fetch_add(1, Ordering::Relaxed);
        let delay_ms = self.get_body_delay_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        let data = self
            .objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("MockS3: not found: {key}"))?;
        Ok(ByteStream::from(data))
    }

    async fn put_bytes(&self, _bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
        self.objects.write().unwrap().insert(key.to_string(), body);
        Ok(())
    }

    async fn put_file(&self, _bucket: &str, key: &str, path: &Path) -> Result<()> {
        self.put_file_calls.fetch_add(1, Ordering::Relaxed);
        if self.put_file_fail.load(Ordering::Relaxed) {
            anyhow::bail!("MockS3: put_file forced failure");
        }
        let data = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading upload source {}", path.display()))?;
        self.objects.write().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn list_keys(&self, _bucket: &str, prefix: &str) -> Result<Vec<(String, u64)>> {
        Ok(self
            .objects
            .read()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.len() as u64))
            .collect())
    }

    async fn delete_object(&self, _bucket: &str, key: &str) -> Result<()> {
        self.objects.write().unwrap().remove(key);
        Ok(())
    }

    async fn put_bytes_if_not_exists(
        &self,
        _bucket: &str,
        key: &str,
        body: Vec<u8>,
    ) -> Result<bool> {
        let mut objects = self.objects.write().unwrap();
        if objects.contains_key(key) {
            Ok(false)
        } else {
            objects.insert(key.to_string(), body);
            Ok(true)
        }
    }
}

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

pub const BUCKET: &str = "b";

/// Create a MockS3 pre-populated with one store's state and optional blocks.
pub fn mock_s3(store_id: &str, state: &State, blocks: &[(u64, Vec<u8>)]) -> MockS3 {
    let s3 = MockS3::new();
    let prefix = "stores";

    let state_key = format!("{prefix}/{store_id}/state.json");
    s3.put(&state_key, serde_json::to_vec(state).unwrap());

    for (idx, data) in blocks {
        let bkey = format!("{prefix}/{store_id}/{idx:016x}");
        s3.put(&bkey, data.clone());
    }

    s3
}

pub async fn load_store(s3: MockS3, store_id: &str) -> Arc<Store<MockS3>> {
    ensure_global_cache().await;
    Store::load(
        s3,
        BUCKET.to_string(),
        "".to_string(),
        store_id.to_string(),
        4,
        4,
    )
    .await
    .expect("Store::load failed")
}

pub async fn wait_for(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    while !predicate() {
        if tokio::time::Instant::now().duration_since(start) > timeout {
            panic!("timed out waiting for condition");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub fn default_state(block_size: u64, volume_size: u64) -> State {
    State {
        parent_id: None,
        block_size,
        volume_size,
        frozen_at: None,
        children: vec![],
    }
}
