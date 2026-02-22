use crate::blockdev_adapter::StoreBlockDevice;
use crate::s3::S3Access;
use crate::store::Store;
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use ext4_lwext4::{Ext4Fs, MkfsOptions, OpenFlags};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;
use tracing_subscriber::EnvFilter;

const BUCKET: &str = "b";
const PREFIX: &str = "";

static CACHE_INIT: OnceCell<()> = OnceCell::const_new();
static ID_SEQ: AtomicU64 = AtomicU64::new(0);
static TRACE_INIT: std::sync::Once = std::sync::Once::new();
static VERBOSE_TEST_LOGS: OnceLock<bool> = OnceLock::new();

fn verbose_test_logs_enabled() -> bool {
    *VERBOSE_TEST_LOGS.get_or_init(|| {
        std::env::var("LOOPHOLE_LWEXT4_DEBUG")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    })
}

fn test_log(msg: &str) {
    if verbose_test_logs_enabled() {
        eprintln!("{msg}");
    }
}

fn unique_id(tag: &str) -> String {
    let n = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{tag}-{n}")
}

async fn ensure_global_cache() {
    CACHE_INIT
        .get_or_init(|| async {
            let dir = std::env::temp_dir().join(format!(
                "loophole_lwext4_no_fuse_test_{}",
                std::process::id()
            ));
            crate::cache::init(dir, 512 * 1024 * 1024)
                .await
                .expect("failed to init test cache");
        })
        .await;
}

#[derive(Clone)]
struct FakeS3 {
    objects: Arc<std::sync::RwLock<HashMap<String, Vec<u8>>>>,
}

impl FakeS3 {
    fn new() -> Self {
        Self {
            objects: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }
}

impl S3Access for FakeS3 {
    async fn get_bytes(&self, _bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("FakeS3: not found: {key}"))
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
            .ok_or_else(|| anyhow::anyhow!("FakeS3: not found: {key}"))?;
        let start = offset as usize;
        let end = (start + len).min(data.len());
        Ok(data[start..end].to_vec())
    }

    async fn get_body(&self, _bucket: &str, key: &str) -> Result<ByteStream> {
        let data = self
            .objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("FakeS3: not found: {key}"))?;
        Ok(ByteStream::from(data))
    }

    async fn put_bytes(&self, _bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
        self.objects.write().unwrap().insert(key.to_string(), body);
        Ok(())
    }

    async fn put_file(&self, _bucket: &str, key: &str, path: &Path) -> Result<()> {
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

async fn load_store(s3: FakeS3, store_id: &str) -> Arc<Store<FakeS3>> {
    ensure_global_cache().await;
    Store::load(
        s3,
        BUCKET.to_string(),
        PREFIX.to_string(),
        store_id.to_string(),
        64,
        64,
    )
    .await
    .expect("Store::load failed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lwext4_filesystem_ops_without_fuse() {
    TRACE_INIT.call_once(|| {
        let mut filter = EnvFilter::from_default_env().add_directive("warn".parse().unwrap());
        if verbose_test_logs_enabled() {
            filter = filter.add_directive("loophole=debug".parse().unwrap());
        }
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(filter)
            .try_init();
    });

    async fn flush_with_timeout(
        store: &Store<FakeS3>,
        label: &str,
    ) -> std::result::Result<(), String> {
        tokio::time::timeout(Duration::from_secs(20), store.flush())
            .await
            .map_err(|_| format!("{label}: store.flush timed out"))?
            .map_err(|e| format!("{label}: store.flush failed: {e:#}"))
    }

    let sid = unique_id("lwext4-no-fuse");
    let s3 = FakeS3::new();
    let block_size = 1024 * 1024;
    let volume_size = 4 * 1024 * 1024;

    test_log("[lwext4-test] format store");
    Store::format(&s3, BUCKET, PREFIX, &sid, block_size, volume_size)
        .await
        .unwrap();

    // Format a real ext4 filesystem through lwext4 on top of Store/FakeS3.
    test_log("[lwext4-test] load store for mkfs");
    let store_for_mkfs = load_store(s3.clone(), &sid).await;
    let device_for_mkfs = StoreBlockDevice::new(
        Arc::clone(&store_for_mkfs),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mkfs_opts = MkfsOptions::ext2().with_block_size(4096);
    test_log("[lwext4-test] run mkfs");
    tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || ext4_lwext4::mkfs(device_for_mkfs, &mkfs_opts)),
    )
    .await
    .expect("mkfs timed out")
    .unwrap()
    .unwrap();
    test_log("[lwext4-test] flush after mkfs");
    flush_with_timeout(&store_for_mkfs, "after mkfs")
        .await
        .unwrap();
    store_for_mkfs.close().await.unwrap();

    // Mount without FUSE and perform varied filesystem operations.
    test_log("[lwext4-test] load store for mount");
    let store = load_store(s3.clone(), &sid).await;
    let device =
        StoreBlockDevice::new(Arc::clone(&store), tokio::runtime::Handle::current()).unwrap();
    test_log("[lwext4-test] mount fs");
    let fs = Ext4Fs::mount(device, false).unwrap();

    test_log("[lwext4-test] basic ops");
    fs.mkdir("/docs", 0o755).unwrap();
    fs.mkdir("/docs/sub", 0o755).unwrap();

    {
        let mut f = fs
            .open("/docs/alpha.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"hello-lwext4").unwrap();
    }

    fs.rename("/docs/alpha.txt", "/docs/final.txt").unwrap();
    fs.link("/docs/final.txt", "/docs/hard.txt").unwrap();
    fs.symlink("/docs/final.txt", "/docs/sym.txt").unwrap();
    assert_eq!(fs.readlink("/docs/sym.txt").unwrap(), "/docs/final.txt");

    {
        let mut f = fs.open("/docs/final.txt", OpenFlags::READ).unwrap();
        let mut out = Vec::new();
        f.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello-lwext4");
    }

    {
        let mut f = fs
            .open("/docs/sub/note.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"nested-note").unwrap();
    }

    fs.sync().unwrap();
    test_log("[lwext4-test] flush after writes");
    flush_with_timeout(&store, "after writes").await.unwrap();
    drop(fs);
    store.close().await.unwrap();

    // Reload and verify persistence.
    test_log("[lwext4-test] reload and remount");
    let store2 = load_store(s3.clone(), &sid).await;
    let device2 =
        StoreBlockDevice::new(Arc::clone(&store2), tokio::runtime::Handle::current()).unwrap();
    let fs2 = Ext4Fs::mount(device2, false).unwrap();

    {
        let mut f = fs2.open("/docs/final.txt", OpenFlags::READ).unwrap();
        let mut out = Vec::new();
        f.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello-lwext4");
    }
    {
        let mut f = fs2.open("/docs/sub/note.txt", OpenFlags::READ).unwrap();
        let mut out = Vec::new();
        f.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"nested-note");
    }

    assert!(fs2.is_file("/docs/final.txt"));
    assert!(fs2.is_dir("/docs/sub"));
    assert!(fs2.stat().unwrap().total_blocks > 0);

    // Cleanup exercises unlink/rmdir.
    fs2.remove("/docs/hard.txt").unwrap();
    fs2.remove("/docs/sym.txt").unwrap();
    fs2.remove("/docs/final.txt").unwrap();
    fs2.remove("/docs/sub/note.txt").unwrap();
    fs2.rmdir("/docs/sub").unwrap();
    fs2.rmdir("/docs").unwrap();
    fs2.sync().unwrap();
    test_log("[lwext4-test] final flush");
    flush_with_timeout(&store2, "final").await.unwrap();
    store2.close().await.unwrap();
    test_log("[lwext4-test] done");
}
