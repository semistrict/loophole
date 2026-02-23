use super::*;
use aws_sdk_s3::primitives::ByteStream;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::OnceCell;

// -----------------------------------------------------------------------
// Global cache – one per test-binary process
// -----------------------------------------------------------------------

static CACHE_INIT: OnceCell<()> = OnceCell::const_new();

async fn ensure_global_cache() {
    CACHE_INIT
        .get_or_init(|| async {
            let dir = std::env::temp_dir().join(format!("loophole_test_{}", std::process::id()));
            crate::cache::init(dir, 512 * 1024 * 1024)
                .await
                .expect("failed to init test cache");
        })
        .await;
}

static ID_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn unique_id(tag: &str) -> String {
    let n = ID_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{tag}-{n}")
}

// -----------------------------------------------------------------------
// In-memory mock S3
// -----------------------------------------------------------------------

#[derive(Clone)]
struct MockS3 {
    objects: Arc<std::sync::RwLock<HashMap<String, Vec<u8>>>>,
    get_body_calls: Arc<AtomicU64>,
    put_file_calls: Arc<AtomicU64>,
    get_body_delay_ms: Arc<AtomicU64>,
    put_file_fail: Arc<AtomicBool>,
}

impl MockS3 {
    fn new() -> Self {
        Self {
            objects: Arc::new(std::sync::RwLock::new(HashMap::new())),
            get_body_calls: Arc::new(AtomicU64::new(0)),
            put_file_calls: Arc::new(AtomicU64::new(0)),
            get_body_delay_ms: Arc::new(AtomicU64::new(0)),
            put_file_fail: Arc::new(AtomicBool::new(false)),
        }
    }

    fn put(&self, key: &str, data: Vec<u8>) {
        self.objects.write().unwrap().insert(key.to_string(), data);
    }

    fn set_get_body_delay_ms(&self, ms: u64) {
        self.get_body_delay_ms.store(ms, Ordering::Relaxed);
    }

    fn get_body_calls(&self) -> u64 {
        self.get_body_calls.load(Ordering::Relaxed)
    }

    fn put_file_calls(&self) -> u64 {
        self.put_file_calls.load(Ordering::Relaxed)
    }

    fn get_object(&self, key: &str) -> Option<Vec<u8>> {
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
}

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

const BUCKET: &str = "b";

/// Create a MockS3 pre-populated with one store's state and optional blocks.
fn mock_s3(store_id: &str, state: &State, blocks: &[(u64, Vec<u8>)]) -> MockS3 {
    let s3 = MockS3::new();
    let prefix = "stores"; // normalize_prefix("") == "stores"

    let state_key = format!("{prefix}/{store_id}/state.json");
    s3.put(&state_key, serde_json::to_vec(state).unwrap());

    for (idx, data) in blocks {
        let bkey = format!("{prefix}/{store_id}/{idx:016x}");
        s3.put(&bkey, data.clone());
    }

    s3
}

// -----------------------------------------------------------------------
// FUSE simulation – exact same block-splitting logic as fs.rs
// -----------------------------------------------------------------------

async fn fuse_read(store: &Store<MockS3>, offset: u64, size: u32) -> Result<Vec<u8>> {
    let volume_size = store.state.volume_size;
    let block_size = store.state.block_size;

    if offset >= volume_size || size == 0 {
        return Ok(vec![]);
    }
    let read_end = offset.saturating_add(size as u64).min(volume_size);
    let first_block = offset / block_size;
    let last_block = (read_end - 1) / block_size;
    let mut buf = Vec::with_capacity((read_end - offset) as usize);

    for block_idx in first_block..=last_block {
        let block_start = block_idx * block_size;
        let slice_start = offset.saturating_sub(block_start);
        let slice_end = (read_end - block_start).min(block_size);
        let len = (slice_end - slice_start) as usize;
        buf.extend_from_slice(&store.do_read_block(block_idx, slice_start, len).await?);
    }
    Ok(buf)
}

async fn fuse_write(store: &Store<MockS3>, offset: u64, data: &[u8]) -> Result<usize> {
    if data.is_empty() {
        return Ok(0);
    }
    let volume_size = store.state.volume_size;
    if offset >= volume_size {
        anyhow::bail!("write starts past end of volume");
    }
    let max_len = (volume_size - offset) as usize;
    let data = &data[..data.len().min(max_len)];
    let block_size = store.state.block_size;
    let first_block = offset / block_size;
    let write_end = offset + data.len() as u64;
    let last_block = (write_end - 1) / block_size;
    let mut data_pos = 0usize;

    for block_idx in first_block..=last_block {
        let block_start = block_idx * block_size;
        let write_start = offset.saturating_sub(block_start);
        let block_end = (write_end - block_start).min(block_size);
        let write_len = (block_end - write_start) as usize;
        let chunk = &data[data_pos..data_pos + write_len];
        data_pos += write_len;
        store.do_write_block(block_idx, write_start, chunk).await?;
    }
    Ok(data.len())
}

// -----------------------------------------------------------------------
// Helper: load a fresh store from mock S3
// -----------------------------------------------------------------------

async fn load_store(s3: MockS3, store_id: &str) -> Arc<Store<MockS3>> {
    ensure_global_cache().await;
    Store::load(
        s3,
        BUCKET.to_string(),
        "".to_string(), // normalizes to "stores"
        store_id.to_string(),
        4, // max_uploads
        4, // max_downloads
    )
    .await
    .expect("Store::load failed")
}

async fn wait_for(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    while !predicate() {
        if tokio::time::Instant::now().duration_since(start) > timeout {
            panic!("timed out waiting for condition");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// =======================================================================
// Integration tests – full pipeline driven like FUSE
// =======================================================================

/// Write a few bytes, read them back through the FUSE path.
/// Everything stays in the cache – no S3 reads.
#[tokio::test]
async fn write_and_readback_single_block() {
    let sid = unique_id("rw1");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    fuse_write(&store, 0, b"hello world").await.unwrap();
    let got = fuse_read(&store, 0, 11).await.unwrap();
    assert_eq!(got, b"hello world");
    store.close().await.unwrap();
}

/// Write data that straddles a block boundary and read it back.
/// With 4 KiB blocks, write 10 bytes starting at offset 4090 →
/// bytes 4090..4095 go to block 0, bytes 4096..4099 go to block 1.
#[tokio::test]
async fn write_and_readback_spanning_block_boundary() {
    let sid = unique_id("span");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let data = b"ABCDEFGHIJ"; // 10 bytes across the 4096 boundary
    fuse_write(&store, 4090, data).await.unwrap();
    let got = fuse_read(&store, 4090, 10).await.unwrap();
    assert_eq!(got, data.as_slice());
    store.close().await.unwrap();
}

/// A larger write spanning three full blocks and partial edges.
/// block_size=64 so arithmetic is easy to follow.
#[tokio::test]
async fn write_and_readback_three_blocks() {
    let sid = unique_id("3blk");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    // Write 150 bytes starting at offset 10.
    // block 0: bytes 10..63  (54 bytes)
    // block 1: bytes 64..127 (64 bytes, full block)
    // block 2: bytes 128..159 (32 bytes)
    let data: Vec<u8> = (0u8..150).collect();
    fuse_write(&store, 10, &data).await.unwrap();

    // Read back the same range.
    let got = fuse_read(&store, 10, 150).await.unwrap();
    assert_eq!(got, data);

    // Read a sub-range that spans blocks 0 and 1.
    let got = fuse_read(&store, 50, 30).await.unwrap();
    assert_eq!(got, &data[40..70]); // offset 50 = data[40], len 30
    store.close().await.unwrap();
}

/// Reading a block that doesn't exist anywhere returns zeros.
#[tokio::test]
async fn read_absent_block_returns_zeros() {
    let sid = unique_id("zeros");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let got = fuse_read(&store, 0, 4096).await.unwrap();
    assert_eq!(got, vec![0u8; 4096]);
    store.close().await.unwrap();
}

/// Read from S3: block exists in the mock S3 index but not in the cache.
#[tokio::test]
async fn read_fetches_from_s3_on_cache_miss() {
    let sid = unique_id("s3fetch");
    let block_data = vec![0xAB; 4096];
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[(0, block_data)]);
    let store = load_store(s3, &sid).await;

    // Block 0 is in the S3 index but not in the local cache, so
    // do_read_block falls through to get_byte_range → MockS3.
    // MockS3 correctly handles range reads, so we get exactly 10 bytes.
    let got = fuse_read(&store, 100, 10).await.unwrap();
    assert_eq!(got, vec![0xAB; 10]);
    store.close().await.unwrap();
}

/// Zero-length read returns empty without touching the store.
#[tokio::test]
async fn zero_length_read_returns_empty() {
    let sid = unique_id("zr");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let got = fuse_read(&store, 0, 0).await.unwrap();
    assert!(got.is_empty());
    store.close().await.unwrap();
}

/// Zero-length write is a no-op.
#[tokio::test]
async fn zero_length_write_is_noop() {
    let sid = unique_id("zw");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let n = fuse_write(&store, 0, b"").await.unwrap();
    assert_eq!(n, 0);
    store.close().await.unwrap();
}

/// Read past the end of the volume should clamp to volume_size.
#[tokio::test]
async fn read_past_eof_clamps_to_volume_size() {
    let sid = unique_id("eof");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 100,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    // Write some data near the end.
    fuse_write(&store, 90, b"ABCDEFGHIJ").await.unwrap(); // fills to 100

    // Ask for 64 bytes starting at 80 — only 20 should come back (80..100).
    let got = fuse_read(&store, 80, 64).await.unwrap();
    assert_eq!(got.len(), 20);
    // First 10 bytes are zeros (80..89), next 10 are our data.
    assert_eq!(&got[..10], &[0u8; 10]);
    assert_eq!(&got[10..], b"ABCDEFGHIJ");
    store.close().await.unwrap();
}

/// Read starting at exactly volume_size returns empty.
#[tokio::test]
async fn read_at_volume_size_returns_empty() {
    let sid = unique_id("ateof");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 100,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let got = fuse_read(&store, 100, 10).await.unwrap();
    assert!(got.is_empty());
    store.close().await.unwrap();
}

/// Write that crosses EOF should be truncated to the remaining volume bytes.
#[tokio::test]
async fn write_past_eof_is_truncated() {
    let sid = unique_id("wteof");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 100,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let n = fuse_write(&store, 95, b"ABCDEFGHIJ").await.unwrap();
    assert_eq!(n, 5);

    let got = fuse_read(&store, 95, 10).await.unwrap();
    assert_eq!(got, b"ABCDE");
    store.close().await.unwrap();
}

/// Write starting exactly at EOF should fail.
#[tokio::test]
async fn write_at_eof_fails() {
    let sid = unique_id("weof");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 100,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let err = fuse_write(&store, 100, b"X").await.unwrap_err();
    assert!(err.to_string().contains("past end of volume"));
    store.close().await.unwrap();
}

/// Writes to a frozen store fail.
#[tokio::test]
async fn write_to_frozen_store_fails() {
    let sid = unique_id("frozen");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec!["child".to_string()],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let err = fuse_write(&store, 0, b"nope").await.unwrap_err();
    assert!(
        err.to_string().contains("frozen"),
        "expected frozen error, got: {err}"
    );
    store.close().await.unwrap();
}

/// Overwriting within one block preserves surrounding zeros.
#[tokio::test]
async fn partial_block_write_preserves_surrounding_data() {
    let sid = unique_id("partial");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    // Write "AB" at offset 10 within block 0.
    fuse_write(&store, 10, b"AB").await.unwrap();

    // Bytes 0..9 should be zero, 10..11 should be "AB", 12..63 should be zero.
    let block = fuse_read(&store, 0, 64).await.unwrap();
    assert_eq!(&block[..10], &[0u8; 10]);
    assert_eq!(&block[10..12], b"AB");
    assert_eq!(&block[12..64], &[0u8; 52]);
    store.close().await.unwrap();
}

/// Multiple small writes to different offsets in the same block.
#[tokio::test]
async fn multiple_writes_same_block_different_offsets() {
    let sid = unique_id("multi");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    fuse_write(&store, 0, b"AAAA").await.unwrap();
    fuse_write(&store, 10, b"BBBB").await.unwrap();
    fuse_write(&store, 60, b"CCCC").await.unwrap(); // crosses into block 1

    let got = fuse_read(&store, 0, 64 + 4).await.unwrap();
    assert_eq!(&got[0..4], b"AAAA");
    assert_eq!(&got[4..10], &[0u8; 6]);
    assert_eq!(&got[10..14], b"BBBB");
    assert_eq!(&got[14..60], &[0u8; 46]);
    // "CCCC" at offset 60: block0 gets 60..63 = "CCC", block1 gets 64 = "C"
    assert_eq!(&got[60..64], &b"CCCC"[..4]);
    store.close().await.unwrap();
}

/// Write exactly at a block boundary (offset is multiple of block_size).
#[tokio::test]
async fn write_at_exact_block_boundary() {
    let sid = unique_id("boundary");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    fuse_write(&store, 64, b"HELLO").await.unwrap();
    let got = fuse_read(&store, 64, 5).await.unwrap();
    assert_eq!(got, b"HELLO");

    // Verify block 0 is still all zeros (untouched).
    let block0 = fuse_read(&store, 0, 64).await.unwrap();
    assert_eq!(block0, vec![0u8; 64]);
    store.close().await.unwrap();
}

/// Concurrent cache misses for the same block should trigger only one
/// background full-block fetch.
#[tokio::test]
async fn concurrent_reads_dedupe_background_fetch() {
    let sid = unique_id("dedupe");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[(0, vec![0xAB; 4096])]);
    s3.set_get_body_delay_ms(100);
    let store = std::sync::Arc::new(load_store(s3.clone(), &sid).await);

    let mut handles = Vec::new();
    for _ in 0..16 {
        let s = std::sync::Arc::clone(&store);
        handles.push(tokio::spawn(async move { fuse_read(&s, 0, 64).await }));
    }
    for h in handles {
        let got = h.await.unwrap().unwrap();
        assert_eq!(got, vec![0xAB; 64]);
    }

    wait_for(Duration::from_secs(2), || s3.get_body_calls() >= 1).await;
    assert_eq!(s3.get_body_calls(), 1);
    store.close().await.unwrap();
}

/// Background full-block streaming read should never cache more than block_size.
#[tokio::test]
async fn background_stream_read_caps_cached_block_size() {
    let sid = unique_id("capread");
    let block_size = 64u64;
    let state = State {
        parent_id: None,
        block_size,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[(0, vec![0xCD; 512])]);
    let store = load_store(s3, &sid).await;

    let got = fuse_read(&store, 0, 1).await.unwrap();
    assert_eq!(got, vec![0xCD]);

    let start = tokio::time::Instant::now();
    loop {
        if crate::cache::get()
            .has_block(&sid, 0)
            .await
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now().duration_since(start) < Duration::from_secs(2),
            "timed out waiting for cached block"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    store.close().await.unwrap();
}

/// Read with an S3 object smaller than block_size.
/// The write path pads cached blocks to block_size before local mutation.
#[tokio::test]
async fn read_s3_block_smaller_than_block_size() {
    let sid = unique_id("short-s3");
    let short_data = vec![0xFE; 100]; // only 100 bytes, block_size = 4096
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[(0, short_data)]);
    let store = load_store(s3, &sid).await;

    // MockS3 correctly handles range reads. The block has 100 bytes of 0xFE,
    // so reading bytes 0..99 returns 0xFE, and bytes 100+ would be zeros
    // (but we only stored 100 bytes so get_byte_range returns 100 bytes for
    // offset=0 len=100).
    let got = fuse_read(&store, 0, 100).await.unwrap();
    assert_eq!(got, vec![0xFE; 100]);
    store.close().await.unwrap();
}

/// Upload path should stream from file (`put_file`) rather than materializing
/// the whole block in heap memory for S3 put.
#[tokio::test]
async fn write_uses_streaming_put_file_upload() {
    let sid = unique_id("putfile");
    let block_size = 64u64;
    let state = State {
        parent_id: None,
        block_size,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    let n = fuse_write(&store, 0, b"HELLO").await.unwrap();
    assert_eq!(n, 5);

    wait_for(Duration::from_secs(2), || s3.put_file_calls() >= 1).await;
    assert!(s3.put_file_calls() >= 1);

    let key = format!("stores/{sid}/0000000000000000");
    let uploaded = s3.get_object(&key).expect("uploaded block missing");
    assert_eq!(uploaded.len(), block_size as usize);
    assert_eq!(&uploaded[..5], b"HELLO");
    store.close().await.unwrap();
}

// =======================================================================
// Snapshot tests
// =======================================================================

/// Snapshot creates a child state in S3 and freezes the parent.
#[tokio::test]
async fn snapshot_creates_child_and_freezes_parent() {
    let sid = unique_id("snap");
    let child_id = format!("{sid}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    store.snapshot(child_id.clone()).await.unwrap();

    // Child state should exist with parent_id pointing back.
    let child_state_key = format!("stores/{child_id}/state.json");
    let child_bytes = s3
        .get_object(&child_state_key)
        .expect("child state missing");
    let child_state: State = serde_json::from_slice(&child_bytes).unwrap();
    assert_eq!(child_state.parent_id.as_deref(), Some(sid.as_str()));
    assert_eq!(child_state.block_size, 64);
    assert_eq!(child_state.volume_size, 1024);
    assert!(child_state.children.is_empty());

    // Parent state should now list the child.
    let parent_state_key = format!("stores/{sid}/state.json");
    let parent_bytes = s3.get_object(&parent_state_key).unwrap();
    let parent_state: State = serde_json::from_slice(&parent_bytes).unwrap();
    assert_eq!(parent_state.children, vec![child_id]);
    store.close().await.unwrap();
}

/// Writes to a store fail after snapshot.
#[tokio::test]
async fn writes_fail_after_snapshot() {
    let sid = unique_id("snapwr");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    fuse_write(&store, 0, b"before").await.unwrap();
    store.snapshot(format!("{sid}-child")).await.unwrap();

    let err = fuse_write(&store, 0, b"after").await.unwrap_err();
    assert!(
        err.to_string().contains("frozen"),
        "expected frozen error, got: {err}"
    );
    store.close().await.unwrap();
}

/// Snapshot flushes dirty blocks to S3 before freezing.
#[tokio::test]
async fn snapshot_flushes_dirty_blocks() {
    let sid = unique_id("snapflush");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    fuse_write(&store, 0, b"dirty data").await.unwrap();

    // Snapshot should flush, so the block must be in S3 afterwards.
    store.snapshot(format!("{sid}-child")).await.unwrap();

    let block_key = format!("stores/{sid}/0000000000000000");
    let uploaded = s3.get_object(&block_key).expect("block not flushed to S3");
    assert_eq!(&uploaded[..10], b"dirty data");
    store.close().await.unwrap();
}

// =======================================================================
// Clone tests
// =======================================================================

/// Clone creates two children and freezes the parent.
#[tokio::test]
async fn clone_creates_two_children_and_freezes() {
    let sid = unique_id("cln");
    let cont_id = format!("{sid}-cont");
    let clone_id = format!("{sid}-clone");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    store
        .clone_store(cont_id.clone(), clone_id.clone())
        .await
        .unwrap();

    // Both children should exist.
    for child_id in [&cont_id, &clone_id] {
        let key = format!("stores/{child_id}/state.json");
        let bytes = s3.get_object(&key).expect("child state missing");
        let child_state: State = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(child_state.parent_id.as_deref(), Some(sid.as_str()));
        assert!(child_state.children.is_empty());
    }

    // Parent should list both children.
    let parent_key = format!("stores/{sid}/state.json");
    let parent_bytes = s3.get_object(&parent_key).unwrap();
    let parent_state: State = serde_json::from_slice(&parent_bytes).unwrap();
    assert_eq!(parent_state.children, vec![cont_id, clone_id]);
    store.close().await.unwrap();
}

/// Writes fail after clone.
#[tokio::test]
async fn writes_fail_after_clone() {
    let sid = unique_id("clnwr");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store
        .clone_store(format!("{sid}-cont"), format!("{sid}-clone"))
        .await
        .unwrap();

    let err = fuse_write(&store, 0, b"nope").await.unwrap_err();
    assert!(
        err.to_string().contains("frozen"),
        "expected frozen error, got: {err}"
    );
    store.close().await.unwrap();
}

/// Clone flushes dirty blocks before freezing.
#[tokio::test]
async fn clone_flushes_dirty_blocks() {
    let sid = unique_id("clnflush");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    fuse_write(&store, 0, b"flush me").await.unwrap();
    store
        .clone_store(format!("{sid}-cont"), format!("{sid}-clone"))
        .await
        .unwrap();

    let block_key = format!("stores/{sid}/0000000000000000");
    let uploaded = s3.get_object(&block_key).expect("block not flushed to S3");
    assert_eq!(&uploaded[..8], b"flush me");
    store.close().await.unwrap();
}

/// A child store can read blocks from its parent's S3 prefix.
#[tokio::test]
async fn child_reads_parent_blocks_after_snapshot() {
    let parent_id = unique_id("sparent");
    let child_id = format!("{parent_id}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&parent_id, &state, &[]);
    let store = load_store(s3.clone(), &parent_id).await;

    // Write data to the parent, then snapshot.
    fuse_write(&store, 0, b"inherited").await.unwrap();
    store.snapshot(child_id.clone()).await.unwrap();

    // Load the child store — it should see the parent's block via ancestry.
    let child = load_store(s3, &child_id).await;
    let got = fuse_read(&child, 0, 9).await.unwrap();
    assert_eq!(got, b"inherited");
    child.close().await.unwrap();
    store.close().await.unwrap();
}

/// A child store can write new data without affecting parent blocks.
#[tokio::test]
async fn child_writes_do_not_affect_parent() {
    let parent_id = unique_id("cowpar");
    let child_id = format!("{parent_id}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&parent_id, &state, &[]);
    let store = load_store(s3.clone(), &parent_id).await;

    fuse_write(&store, 0, b"original").await.unwrap();
    store.snapshot(child_id.clone()).await.unwrap();

    // Write different data to the child at the same offset.
    let child = load_store(s3.clone(), &child_id).await;
    fuse_write(&child, 0, b"modified").await.unwrap();

    let child_data = fuse_read(&child, 0, 8).await.unwrap();
    assert_eq!(child_data, b"modified");

    // Parent's S3 block should still have the original data.
    let parent_block_key = format!("stores/{parent_id}/0000000000000000");
    let parent_s3_data = s3.get_object(&parent_block_key).unwrap();
    assert_eq!(&parent_s3_data[..8], b"original");
    child.close().await.unwrap();
    store.close().await.unwrap();
}

// =======================================================================
// Ctl (virtual control directory) tests
// =======================================================================

/// Ctl: snapshot via virtual file creation.
#[tokio::test]
async fn ctl_snapshot() {
    let sid = unique_id("ctlsn");
    let child_id = format!("{sid}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    let ctl = crate::ctl::Ctl::new(Arc::clone(&store));
    let result = ctl.create(crate::ctl::SNAPSHOTS_DIR_INO, &child_id).await;
    assert!(result.is_some());
    let (ino, _size) = result.unwrap().expect("snapshot should succeed");

    // Read back status JSON.
    let (data, _eof) = ctl
        .read(ino, 0, 4096)
        .expect("virtual file should be readable");
    let status: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(status["status"], "complete");

    // Verify child state was created in S3.
    let child_key = format!("stores/{child_id}/state.json");
    assert!(s3.get_object(&child_key).is_some());
    store.close().await.unwrap();
}

/// Ctl: clone via virtual file creation.
#[tokio::test]
async fn ctl_clone() {
    let sid = unique_id("ctlcl");
    let clone_id = format!("{sid}-clone");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    let ctl = crate::ctl::Ctl::new(Arc::clone(&store));
    let result = ctl.create(crate::ctl::CLONES_DIR_INO, &clone_id).await;
    assert!(result.is_some());
    let (ino, _size) = result.unwrap().expect("clone should succeed");

    // Read back status JSON — should include continuation ID.
    let (data, _eof) = ctl
        .read(ino, 0, 4096)
        .expect("virtual file should be readable");
    let status: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(status["status"], "complete");
    assert!(
        status["continuation"].is_string(),
        "should have continuation ID"
    );

    // Both clone and continuation should exist in S3.
    let continuation_id = status["continuation"].as_str().unwrap();
    let clone_key = format!("stores/{clone_id}/state.json");
    let cont_key = format!("stores/{continuation_id}/state.json");
    assert!(s3.get_object(&clone_key).is_some(), "missing clone state");
    assert!(
        s3.get_object(&cont_key).is_some(),
        "missing continuation state"
    );
    store.close().await.unwrap();
}

/// Ctl: snapshot on an already-frozen store returns error status.
#[tokio::test]
async fn ctl_snapshot_on_frozen_store_fails() {
    let sid = unique_id("ctlfr");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec!["existing-child".to_string()],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let ctl = crate::ctl::Ctl::new(Arc::clone(&store));
    let result = ctl
        .create(crate::ctl::SNAPSHOTS_DIR_INO, "another-child")
        .await;
    assert!(result.is_some());
    let (ino, _size) = result
        .unwrap()
        .expect("create returns Ok even on error status");

    // Read back status — should be error.
    let (data, _eof) = ctl
        .read(ino, 0, 4096)
        .expect("virtual file should be readable");
    let status: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(status["status"], "error");
    assert!(status["error"].is_string());
    assert!(store.frozen.load(Ordering::Acquire));
    store.close().await.unwrap();
}

// =======================================================================
// Zero-block / tombstone tests
// =======================================================================

/// Zero a block with no ancestor — the S3 object should be deleted and reads return zeros.
#[tokio::test]
async fn test_zero_block_no_ancestor() {
    let sid = unique_id("zb-noancestor");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    // Write data, flush to S3.
    fuse_write(&store, 0, b"hello world").await.unwrap();
    store.flush().await.unwrap();

    let block_key = format!("stores/{sid}/0000000000000000");
    assert!(
        s3.get_object(&block_key).is_some(),
        "block should be in S3 after flush"
    );

    // Zero the block and flush so the deferred S3 delete completes.
    store.do_zero_block(0).await.unwrap();
    store.flush().await.unwrap();

    // S3 object should be deleted (no ancestor → absence = zeros).
    assert!(
        s3.get_object(&block_key).is_none(),
        "block should be deleted from S3"
    );

    // Read should return zeros.
    let got = fuse_read(&store, 0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    store.close().await.unwrap();
}

/// Zero a block when parent has the block — a 0-byte tombstone is written.
#[tokio::test]
async fn test_zero_block_with_ancestor() {
    let parent_id = unique_id("zb-parent");
    let child_id = format!("{parent_id}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&parent_id, &state, &[]);
    let store = load_store(s3.clone(), &parent_id).await;

    // Write data to parent, snapshot.
    fuse_write(&store, 0, b"parent data!").await.unwrap();
    store.snapshot(child_id.clone()).await.unwrap();

    // Load child.
    let child = load_store(s3.clone(), &child_id).await;

    // Verify child can read parent data.
    let got = fuse_read(&child, 0, 12).await.unwrap();
    assert_eq!(got, b"parent data!");

    // Zero the block in the child and flush so the deferred tombstone write completes.
    child.do_zero_block(0).await.unwrap();
    child.flush().await.unwrap();

    // A 0-byte tombstone should exist in the child's S3 prefix.
    let child_block_key = format!("stores/{child_id}/0000000000000000");
    let tombstone = s3
        .get_object(&child_block_key)
        .expect("tombstone should exist");
    assert_eq!(tombstone.len(), 0, "tombstone should be 0 bytes");

    // Read should now return zeros.
    let got = fuse_read(&child, 0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    child.close().await.unwrap();
    store.close().await.unwrap();
}

/// read after zero_block returns zeros.
#[tokio::test]
async fn test_read_zero_block_returns_zeros() {
    let sid = unique_id("zb-read");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    fuse_write(&store, 0, b"data here").await.unwrap();
    store.do_zero_block(0).await.unwrap();

    let got = fuse_read(&store, 0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    store.close().await.unwrap();
}

/// Writing a full block of zeros triggers the zero_block path.
#[tokio::test]
async fn test_write_full_zeros_triggers_zero_block() {
    let parent_id = unique_id("zb-fullzero-p");
    let child_id = format!("{parent_id}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&parent_id, &state, &[]);
    let store = load_store(s3.clone(), &parent_id).await;

    // Write data to parent, snapshot.
    fuse_write(&store, 0, b"some data").await.unwrap();
    store.snapshot(child_id.clone()).await.unwrap();

    // Load child and write a full block of zeros, then flush.
    let child = load_store(s3.clone(), &child_id).await;
    fuse_write(&child, 0, &[0u8; 64]).await.unwrap();
    child.flush().await.unwrap();

    // The child should have a tombstone (0-byte object), not a 64-byte zero block.
    let child_block_key = format!("stores/{child_id}/0000000000000000");
    let obj = s3
        .get_object(&child_block_key)
        .expect("tombstone should exist");
    assert_eq!(obj.len(), 0, "should be tombstone, not full zero block");

    // Read should return zeros.
    let got = fuse_read(&child, 0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    child.close().await.unwrap();
    store.close().await.unwrap();
}

/// Zero a block, then write real data — data is readable and zero_blocks is cleared.
#[tokio::test]
async fn test_zero_block_then_write_data() {
    let sid = unique_id("zb-rewrite");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    fuse_write(&store, 0, b"initial").await.unwrap();
    store.do_zero_block(0).await.unwrap();

    // Confirm zeros.
    let got = fuse_read(&store, 0, 7).await.unwrap();
    assert_eq!(got, vec![0u8; 7]);

    // Write new data.
    fuse_write(&store, 0, b"restored").await.unwrap();

    let got = fuse_read(&store, 0, 8).await.unwrap();
    assert_eq!(got, b"restored");

    // zero_blocks should no longer contain this block.
    assert!(!store.zero_blocks.contains(&0));
    store.close().await.unwrap();
}

/// Tombstone survives a store reload — zero_blocks populated from S3 listing.
#[tokio::test]
async fn test_tombstone_survives_reload() {
    let parent_id = unique_id("zb-reload-p");
    let child_id = format!("{parent_id}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&parent_id, &state, &[]);
    let store = load_store(s3.clone(), &parent_id).await;

    fuse_write(&store, 0, b"parent data").await.unwrap();
    store.snapshot(child_id.clone()).await.unwrap();

    let child = load_store(s3.clone(), &child_id).await;
    child.do_zero_block(0).await.unwrap();

    // Flush and reload the child.
    child.flush().await.unwrap();
    let child2 = load_store(s3.clone(), &child_id).await;

    // zero_blocks should be populated from the tombstone.
    assert!(
        child2.zero_blocks.contains(&0),
        "zero_blocks should contain block 0 after reload"
    );

    // Reading should still return zeros.
    let got = fuse_read(&child2, 0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    child2.close().await.unwrap();
    child.close().await.unwrap();
    store.close().await.unwrap();
}

/// Write 7.5 blocks of zeros starting halfway through a block.
/// Full interior blocks should be tombstoned, partial edge blocks should be
/// normal writes with the non-written half preserved.
#[tokio::test]
async fn test_write_zeros_spanning_partial_and_full_blocks() {
    let block_size: u64 = 64;
    let sid = unique_id("zb-span");
    let state = State {
        parent_id: None,
        block_size,
        volume_size: block_size * 40,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    // Fill blocks 23..=32 with non-zero data so we can verify partial preservation.
    let fill: Vec<u8> = (0..block_size as u8)
        .cycle()
        .take((block_size * 10) as usize)
        .collect();
    let fill_offset = 23 * block_size;
    fuse_write(&store, fill_offset, &fill).await.unwrap();
    store.flush().await.unwrap();

    // Write zeros from 1/4 into block 23 through 3/4 into block 31.
    // offset = 23*64 + 16 = 1488, end = 31*64 + 48 = 2032, len = 544.
    // This covers:
    //   block 23: partial (last 48 bytes zeroed, first 16 preserved)
    //   blocks 24..=30: full blocks → tombstoned
    //   block 31: partial (first 48 bytes zeroed, last 16 preserved)
    let quarter = block_size / 4;
    let zero_offset = 23 * block_size + quarter;
    let zero_end = 31 * block_size + 3 * quarter;
    let zero_len = (zero_end - zero_offset) as usize;
    let zeros = vec![0u8; zero_len];
    fuse_write(&store, zero_offset, &zeros).await.unwrap();

    // Full interior blocks 24..=30 should be in zero_blocks.
    for blk in 24..=30 {
        assert!(
            store.zero_blocks.contains(&blk),
            "block {blk} should be in zero_blocks"
        );
    }

    // Partial edge blocks 23 and 31 should NOT be in zero_blocks — they went
    // through the normal write path because only part of each was zeroed.
    assert!(
        !store.zero_blocks.contains(&23),
        "block 23 (partial) should not be in zero_blocks"
    );
    assert!(
        !store.zero_blocks.contains(&31),
        "block 31 (partial) should not be in zero_blocks"
    );

    // Read back block 23: first 16 bytes = original fill, last 48 bytes = zeros.
    let b23 = fuse_read(&store, 23 * block_size, block_size as u32)
        .await
        .unwrap();
    let expected_b23_head: Vec<u8> = (0..quarter as u8).collect();
    assert_eq!(&b23[..quarter as usize], &expected_b23_head[..]);
    assert_eq!(
        &b23[quarter as usize..],
        vec![0u8; (block_size - quarter) as usize]
    );

    // Read back block 31: first 48 bytes = zeros, last 16 bytes = original fill.
    let b31 = fuse_read(&store, 31 * block_size, block_size as u32)
        .await
        .unwrap();
    assert_eq!(
        &b31[..(3 * quarter) as usize],
        vec![0u8; (3 * quarter) as usize]
    );
    let expected_b31_tail: Vec<u8> = (3 * quarter as u8..block_size as u8).collect();
    assert_eq!(&b31[(3 * quarter) as usize..], &expected_b31_tail[..]);

    // Full zero blocks should read back as all zeros.
    for blk in 24..=30 {
        let data = fuse_read(&store, blk * block_size, block_size as u32)
            .await
            .unwrap();
        assert_eq!(
            data,
            vec![0u8; block_size as usize],
            "block {blk} should read as zeros"
        );
    }
    store.close().await.unwrap();
}

// =======================================================================
// Store ID validation tests
// =======================================================================

#[tokio::test]
async fn test_format_rejects_store_id_with_slash() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::format(&s3, BUCKET, "", "../../etc", 64, 1024).await;
    assert!(
        result.is_err(),
        "store ID with path traversal should be rejected"
    );
}

#[tokio::test]
async fn test_format_rejects_store_id_with_backslash() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::format(&s3, BUCKET, "", "foo\\bar", 64, 1024).await;
    assert!(
        result.is_err(),
        "store ID with backslash should be rejected"
    );
}

#[tokio::test]
async fn test_format_rejects_empty_store_id() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::format(&s3, BUCKET, "", "", 64, 1024).await;
    assert!(result.is_err(), "empty store ID should be rejected");
}

#[tokio::test]
async fn test_format_rejects_dot_dot_store_id() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::format(&s3, BUCKET, "", "..", 64, 1024).await;
    assert!(result.is_err(), "'..' store ID should be rejected");
}

#[tokio::test]
async fn test_load_rejects_invalid_store_id() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::load(
        s3,
        BUCKET.to_string(),
        "".to_string(),
        "../evil".to_string(),
        4,
        4,
    )
    .await;
    assert!(
        result.is_err(),
        "load with path traversal store ID should be rejected"
    );
}

// =======================================================================
// zero_range tests
// =======================================================================

/// zero_range on a partial block preserves the rest of the data.
#[tokio::test]
async fn test_zero_range_partial_block() {
    let sid = unique_id("zr-partial");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    // Write known data.
    let data: Vec<u8> = (0..64u8).collect();
    fuse_write(&store, 0, &data).await.unwrap();

    // Zero bytes 16..48 within the block.
    store.do_zero_range(0, 16, 32).await.unwrap();

    // Read back: first 16 bytes preserved, middle 32 zeroed, last 16 preserved.
    let got = fuse_read(&store, 0, 64).await.unwrap();
    assert_eq!(&got[..16], &(0..16u8).collect::<Vec<_>>());
    assert_eq!(&got[16..48], &[0u8; 32]);
    assert_eq!(&got[48..64], &(48..64u8).collect::<Vec<_>>());
    store.close().await.unwrap();
}

/// zero_range covering a full block delegates to do_zero_block (tombstone path).
#[tokio::test]
async fn test_zero_range_full_block_becomes_tombstone() {
    let parent_id = unique_id("zr-full-parent");
    let child_id = format!("{parent_id}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&parent_id, &state, &[]);
    let store = load_store(s3.clone(), &parent_id).await;

    fuse_write(&store, 0, b"parent data here!").await.unwrap();
    store.snapshot(child_id.clone()).await.unwrap();

    let child = load_store(s3.clone(), &child_id).await;

    // zero_range with offset=0, len=block_size should tombstone.
    child.do_zero_range(0, 0, 64).await.unwrap();

    assert!(
        child.zero_blocks.contains(&0),
        "full-block zero_range should tombstone"
    );
    let got = fuse_read(&child, 0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    child.close().await.unwrap();
    store.close().await.unwrap();
}

/// Zero a block then immediately write real data. After flush, S3 must contain
/// the real data — the deferred tombstone/delete must not clobber the write.
#[tokio::test]
async fn test_zero_block_then_immediate_write_survives_flush() {
    let parent_id = unique_id("zb-race-p");
    let child_id = format!("{parent_id}-child");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        children: vec![],
    };
    let s3 = mock_s3(&parent_id, &state, &[]);
    let store = load_store(s3.clone(), &parent_id).await;

    // Parent writes data, then snapshots.
    fuse_write(&store, 0, b"parent-data-here").await.unwrap();
    store.snapshot(child_id.clone()).await.unwrap();

    let child = load_store(s3.clone(), &child_id).await;

    // Zero the block (enqueues tombstone work).
    child.do_zero_block(0).await.unwrap();
    assert!(child.zero_blocks.contains(&0));

    // Immediately write real data (should clear zero_blocks and enqueue upload).
    fuse_write(&child, 0, b"new-real-data!!!").await.unwrap();
    assert!(
        !child.zero_blocks.contains(&0),
        "write should clear zero_blocks"
    );

    // Flush — both the zero and the write are in the upload queue.
    child.flush().await.unwrap();

    // S3 must contain the real data, not a tombstone.
    let child_block_key = format!("stores/{child_id}/0000000000000000");
    let obj = s3
        .get_object(&child_block_key)
        .expect("block should exist in S3");
    assert!(!obj.is_empty(), "block should not be a tombstone");
    assert_eq!(&obj[..16], b"new-real-data!!!");

    // Reload the child and verify data survives.
    let child2 = load_store(s3.clone(), &child_id).await;
    let got = fuse_read(&child2, 0, 16).await.unwrap();
    assert_eq!(got, b"new-real-data!!!");
    child2.close().await.unwrap();
    child.close().await.unwrap();
    store.close().await.unwrap();
}

/// Reproduce the fsx_heavy race: write a block, flush it to S3, evict
/// the cache file, zero the block (which enqueues deferred S3 work),
/// then immediately do a partial write. The partial write needs to
/// read-modify-write but the cache file was deleted by do_zero_block.
/// It must NOT try to fetch from S3 — it should treat the block as
/// zeroed and create a sparse file instead.
#[tokio::test]
async fn test_zero_block_then_partial_write_no_cache() {
    let sid = unique_id("zero-partial");
    let state = State {
        parent_id: None,
        block_size: 4096,
        volume_size: 1024 * 1024,
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    // 1. Write a full block of non-zero data.
    let original = vec![0xAA; 4096];
    fuse_write(&store, 0, &original).await.unwrap();

    // 2. Flush so it gets uploaded to S3 and is in local_index.
    store.flush().await.unwrap();
    let block_key = format!("stores/{sid}/0000000000000000");
    assert!(s3.get_object(&block_key).is_some(), "block should be in S3");

    // 3. Zero the block. This deletes the cache file and enqueues deferred
    //    S3 work (delete, since no ancestor). The block is now in both
    //    zero_blocks AND local_index until the uploader processes it.
    store.do_zero_block(0).await.unwrap();
    assert!(store.zero_blocks.contains(&0));

    // 4. Immediately do a partial write (not full block).
    //    This must succeed even though the cache file is gone.
    fuse_write(&store, 10, b"hello").await.unwrap();

    // 5. Read back — should see zeros + "hello" at offset 10.
    let got = fuse_read(&store, 0, 4096).await.unwrap();
    assert_eq!(&got[0..10], &[0u8; 10], "bytes before write should be zero");
    assert_eq!(&got[10..15], b"hello", "written bytes should be present");
    assert_eq!(
        &got[15..4096],
        &vec![0u8; 4096 - 15],
        "bytes after write should be zero"
    );

    // 6. Flush and verify.
    store.flush().await.unwrap();
    let obj = s3
        .get_object(&block_key)
        .expect("block should exist after flush");
    assert_eq!(&obj[10..15], b"hello");
    store.close().await.unwrap();
}
