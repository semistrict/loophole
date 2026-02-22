use crate::blockdev_adapter::StoreBlockDevice;
use crate::s3::S3Access;
use crate::store::Store;
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use ext4_lwext4::{Ext4Fs, MkfsOptions, OpenFlags};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::OnceCell;

const BUCKET: &str = "b";
const PREFIX: &str = "";
const DEFAULT_BLOCK_SIZE: u64 = 1024 * 1024;
const DEFAULT_VOLUME_SIZE: u64 = 64 * 1024 * 1024;

static CACHE_INIT: OnceCell<()> = OnceCell::const_new();
static ID_SEQ: AtomicU64 = AtomicU64::new(0);
static ASSERT_INIT: Once = Once::new();

fn unique_id(tag: &str) -> String {
    let n = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{tag}-{n}")
}

async fn ensure_global_cache() {
    ASSERT_INIT.call_once(crate::assert::init);
    CACHE_INIT
        .get_or_init(|| async {
            let dir = std::env::temp_dir().join(format!("loophole_lwext4_py_port_{}", std::process::id()));
            crate::cache::init(dir, 8 * 1024 * 1024 * 1024)
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

async fn format_and_mkfs(
    s3: &FakeS3,
    store_id: &str,
    block_size: u64,
    volume_size: u64,
) -> Arc<Store<FakeS3>> {
    Store::format(s3, BUCKET, PREFIX, store_id, block_size, volume_size)
        .await
        .expect("Store::format failed");
    let store = load_store(s3.clone(), store_id).await;
    let device = StoreBlockDevice::new(Arc::clone(&store), tokio::runtime::Handle::current())
        .expect("StoreBlockDevice::new failed");
    let opts = MkfsOptions::ext2().with_block_size(4096);
    tokio::task::spawn_blocking(move || ext4_lwext4::mkfs(device, &opts))
        .await
        .expect("join mkfs")
        .expect("mkfs failed");
    store.flush().await.expect("flush after mkfs");
    store.close().await.expect("close after mkfs");
    load_store(s3.clone(), store_id).await
}

fn mount_fs(store: &Arc<Store<FakeS3>>, read_only: bool) -> Ext4Fs {
    let device = StoreBlockDevice::new(Arc::clone(store), tokio::runtime::Handle::current())
        .expect("StoreBlockDevice::new failed");
    Ext4Fs::mount(device, read_only).expect("Ext4Fs::mount failed")
}

fn make_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| ((i * 131 + 17) % 251) as u8).collect()
}

fn write_standard_files(fs: &Ext4Fs) -> Vec<u8> {
    fs.mkdir("/subdir", 0o755).unwrap();
    fs.mkdir("/subdir/nested", 0o755).unwrap();

    {
        let mut f = fs
            .open("/greeting.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"hello from loophole\n").unwrap();
    }
    {
        let random = make_bytes(2 * 1024 * 1024);
        let mut f = fs
            .open("/random.bin", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(&random).unwrap();
        let mut deep = fs
            .open("/subdir/nested/deep.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        deep.write_all(b"nested file\n").unwrap();
        let mut nums = fs
            .open("/numbers.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        for i in 1..=1000u32 {
            nums.write_all(format!("{i}\n").as_bytes()).unwrap();
        }
        random
    }
}

fn verify_standard_files(fs: &Ext4Fs, random: &[u8]) {
    let mut greeting = fs.open("/greeting.txt", OpenFlags::READ).unwrap();
    let mut greeting_buf = Vec::new();
    greeting.read_to_end(&mut greeting_buf).unwrap();
    assert_eq!(greeting_buf, b"hello from loophole\n");

    let mut rand_file = fs.open("/random.bin", OpenFlags::READ).unwrap();
    let mut rand_buf = Vec::new();
    rand_file.read_to_end(&mut rand_buf).unwrap();
    assert_eq!(rand_buf, random);

    let mut deep = fs.open("/subdir/nested/deep.txt", OpenFlags::READ).unwrap();
    let mut deep_buf = Vec::new();
    deep.read_to_end(&mut deep_buf).unwrap();
    assert_eq!(deep_buf, b"nested file\n");

    let mut nums = fs.open("/numbers.txt", OpenFlags::READ).unwrap();
    let mut nums_text = String::new();
    nums.read_to_string(&mut nums_text).unwrap();
    assert_eq!(nums_text.lines().count(), 1000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_store_format_variants() {
    let s3 = FakeS3::new();
    Store::format(
        &s3,
        BUCKET,
        PREFIX,
        &unique_id("fmt-default"),
        4 * 1024 * 1024,
        1024 * 1024 * 1024,
    )
    .await
    .unwrap();

    let sid = unique_id("fmt-custom");
    Store::format(&s3, BUCKET, PREFIX, &sid, 1024 * 1024, 64 * 1024 * 1024)
        .await
        .unwrap();
    let store = load_store(s3.clone(), &sid).await;
    assert_eq!(store.state.block_size, 1024 * 1024);
    assert_eq!(store.state.volume_size, 64 * 1024 * 1024);
    store.close().await.unwrap();

    let sid_large = unique_id("fmt-large");
    Store::format(
        &s3,
        BUCKET,
        PREFIX,
        &sid_large,
        4 * 1024 * 1024,
        10 * 1024 * 1024 * 1024,
    )
    .await
    .unwrap();
    let large = load_store(s3.clone(), &sid_large).await;
    assert_eq!(large.state.volume_size, 10 * 1024 * 1024 * 1024);
    large.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_ext4_small_text_file() {
    let s3 = FakeS3::new();
    let sid = unique_id("ext4rw");
    let store = format_and_mkfs(&s3, &sid, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs = mount_fs(&store, false);

    {
        let mut f = fs
            .open("/greeting.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"hello from loophole\n").unwrap();
    }
    fs.sync().unwrap();
    store.flush().await.unwrap();

    {
        let mut f = fs.open("/greeting.txt", OpenFlags::READ).unwrap();
        let mut content = String::new();
        f.read_to_string(&mut content).unwrap();
        assert_eq!(content, "hello from loophole\n");
    }

    drop(fs);
    store.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_ext4_binary_file_integrity() {
    let s3 = FakeS3::new();
    let sid = unique_id("ext4bin");
    let store = format_and_mkfs(&s3, &sid, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs = mount_fs(&store, false);

    let data = make_bytes(3 * 1024 * 1024);
    {
        let mut f = fs
            .open("/random.bin", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(&data).unwrap();
    }
    fs.sync().unwrap();
    store.flush().await.unwrap();

    {
    {
        let mut f = fs.open("/random.bin", OpenFlags::READ).unwrap();
        let mut read_back = Vec::new();
        f.read_to_end(&mut read_back).unwrap();
        assert_eq!(read_back, data);
    }
    }

    drop(fs);
    store.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_ext4_nested_dirs_and_large_seq_file() {
    let s3 = FakeS3::new();
    let sid = unique_id("ext4-nested-seq");
    let store = format_and_mkfs(&s3, &sid, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs = mount_fs(&store, false);

    fs.mkdir("/subdir", 0o755).unwrap();
    fs.mkdir("/subdir/nested", 0o755).unwrap();
    {
        let mut f = fs
            .open("/subdir/nested/deep.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"nested file\n").unwrap();
    }
    {
        let mut f = fs
            .open("/numbers.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        for i in 1..=1000u32 {
            f.write_all(format!("{i}\n").as_bytes()).unwrap();
        }
    }
    fs.sync().unwrap();
    store.flush().await.unwrap();

    {
    {
        let mut deep = fs.open("/subdir/nested/deep.txt", OpenFlags::READ).unwrap();
        let mut deep_buf = String::new();
        deep.read_to_string(&mut deep_buf).unwrap();
        assert_eq!(deep_buf, "nested file\n");
    }

    {
        let mut numbers = fs.open("/numbers.txt", OpenFlags::READ).unwrap();
        let mut numbers_text = String::new();
        numbers.read_to_string(&mut numbers_text).unwrap();
        assert_eq!(numbers_text.lines().count(), 1000);
    }
    }

    drop(fs);
    store.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_ext4_overwrite_and_delete_recreate() {
    let s3 = FakeS3::new();
    let sid = unique_id("ext4-overwrite-delete");
    let store = format_and_mkfs(&s3, &sid, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs = mount_fs(&store, false);

    {
        let mut f = fs
            .open("/overwrite.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"version 1\n").unwrap();
    }
    fs.sync().unwrap();
    {
        let mut f = fs
            .open(
                "/overwrite.txt",
                OpenFlags::WRITE | OpenFlags::TRUNCATE | OpenFlags::CREATE,
            )
            .unwrap();
        f.write_all(b"version 2\n").unwrap();
    }
    fs.sync().unwrap();

    {
        let mut f = fs.open("/overwrite.txt", OpenFlags::READ).unwrap();
        let mut content = String::new();
        f.read_to_string(&mut content).unwrap();
        assert_eq!(content, "version 2\n");
    }

    {
        let mut e = fs
            .open("/ephemeral.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        e.write_all(b"exists\n").unwrap();
    }
    fs.sync().unwrap();
    fs.remove("/ephemeral.txt").unwrap();
    assert!(!fs.exists("/ephemeral.txt"));
    {
        let mut e = fs
            .open("/ephemeral.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        e.write_all(b"back again\n").unwrap();
    }
    fs.sync().unwrap();

    {
        let mut e = fs.open("/ephemeral.txt", OpenFlags::READ).unwrap();
        let mut econtent = String::new();
        e.read_to_string(&mut econtent).unwrap();
        assert_eq!(econtent, "back again\n");
    }

    store.flush().await.unwrap();
    drop(fs);
    store.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_ext4_files_survive_remount() {
    let sid = unique_id("persist");
    let s3 = FakeS3::new();

    let store1 = format_and_mkfs(&s3, &sid, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs1 = mount_fs(&store1, false);
    let random = write_standard_files(&fs1);
    fs1.sync().unwrap();
    store1.flush().await.unwrap();
    drop(fs1);
    store1.close().await.unwrap();

    let store2 = load_store(s3.clone(), &sid).await;
    let fs2 = mount_fs(&store2, false);
    verify_standard_files(&fs2, &random);
    drop(fs2);
    store2.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_snapshot_preserves_data_and_child_writes_independently() {
    let parent_id = unique_id("snap-parent");
    let child_id = unique_id("snap-child");
    let s3 = FakeS3::new();

    let parent = format_and_mkfs(&s3, &parent_id, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs_parent = mount_fs(&parent, false);
    let random = write_standard_files(&fs_parent);
    {
        let mut p = fs_parent
            .open("/parent.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        p.write_all(b"from parent\n").unwrap();
    }
    fs_parent.sync().unwrap();
    parent.flush().await.unwrap();
    drop(fs_parent);

    parent.snapshot(child_id.clone()).await.unwrap();
    parent.close().await.unwrap();

    let child = load_store(s3.clone(), &child_id).await;
    let fs_child = mount_fs(&child, false);
    verify_standard_files(&fs_child, &random);
    {
        let mut c = fs_child
            .open("/child.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        c.write_all(b"from child\n").unwrap();
    }
    fs_child.sync().unwrap();
    child.flush().await.unwrap();
    drop(fs_child);
    child.close().await.unwrap();

    let parent_ro = load_store(s3.clone(), &parent_id).await;
    let fs_parent_ro = mount_fs(&parent_ro, true);
    assert!(fs_parent_ro.exists("/parent.txt"));
    assert!(!fs_parent_ro.exists("/child.txt"));
    drop(fs_parent_ro);
    parent_ro.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_snapshot_freezes_parent_writes() {
    let parent_id = unique_id("snap-frozen");
    let child_id = unique_id("snap-frozen-c");
    let s3 = FakeS3::new();

    let parent = format_and_mkfs(&s3, &parent_id, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs_parent = mount_fs(&parent, false);
    {
        let mut f = fs_parent
            .open("/data.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"seed\n").unwrap();
    }
    fs_parent.sync().unwrap();
    parent.flush().await.unwrap();
    drop(fs_parent);

    parent.snapshot(child_id).await.unwrap();

    let fs_frozen = mount_fs(&parent, true);
    let write_result = fs_frozen
        .open("/should-fail.txt", OpenFlags::CREATE | OpenFlags::WRITE)
        .and_then(|mut f| {
            f.write_all(b"x")?;
            f.sync()
        });
    assert!(write_result.is_err(), "write to frozen parent should fail");
    drop(fs_frozen);
    parent.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_clone_preserves_data_and_branches_are_independent() {
    let parent_id = unique_id("cln-parent");
    let continuation_id = unique_id("cln-cont");
    let clone_id = unique_id("cln-clone");
    let s3 = FakeS3::new();

    let parent = format_and_mkfs(&s3, &parent_id, DEFAULT_BLOCK_SIZE, DEFAULT_VOLUME_SIZE).await;
    let fs_parent = mount_fs(&parent, false);
    let random = write_standard_files(&fs_parent);
    {
        let mut shared = fs_parent
            .open("/shared.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        shared.write_all(b"from parent\n").unwrap();
    }
    fs_parent.sync().unwrap();
    parent.flush().await.unwrap();
    drop(fs_parent);

    parent
        .clone_store(continuation_id.clone(), clone_id.clone())
        .await
        .unwrap();
    parent.close().await.unwrap();

    let cont = load_store(s3.clone(), &continuation_id).await;
    let fs_cont = mount_fs(&cont, false);
    verify_standard_files(&fs_cont, &random);
    {
        let mut f = fs_cont
            .open("/cont.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"continuation only\n").unwrap();
    }
    fs_cont.sync().unwrap();
    cont.flush().await.unwrap();
    drop(fs_cont);
    cont.close().await.unwrap();

    let clone = load_store(s3.clone(), &clone_id).await;
    let fs_clone = mount_fs(&clone, false);
    verify_standard_files(&fs_clone, &random);
    {
        let mut f = fs_clone
            .open("/clone.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"clone only\n").unwrap();
    }
    fs_clone.sync().unwrap();
    clone.flush().await.unwrap();

    assert!(fs_clone.exists("/shared.txt"));
    assert!(fs_clone.exists("/clone.txt"));
    assert!(!fs_clone.exists("/cont.txt"));
    drop(fs_clone);
    clone.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn py_port_high_level_like_format_and_nested_large_file() {
    let s3 = FakeS3::new();
    let sid = unique_id("hlfmt");
    let store = format_and_mkfs(&s3, &sid, 1024 * 1024, 64 * 1024 * 1024).await;
    let fs = mount_fs(&store, false);

    {
        let mut f = fs
            .open("/hello.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        f.write_all(b"formatted via high-level\n").unwrap();
    }
    fs.mkdir("/a", 0o755).unwrap();
    fs.mkdir("/a/b", 0o755).unwrap();
    fs.mkdir("/a/b/c", 0o755).unwrap();
    {
        let mut deep = fs
            .open("/a/b/c/deep.txt", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        deep.write_all(b"deep nested\n").unwrap();
    }
    let big = make_bytes(2 * 1024 * 1024);
    {
        let mut bigf = fs
            .open("/a/big.bin", OpenFlags::CREATE | OpenFlags::WRITE)
            .unwrap();
        bigf.write_all(&big).unwrap();
    }
    fs.sync().unwrap();
    store.flush().await.unwrap();

    {
        let mut hello = fs.open("/hello.txt", OpenFlags::READ).unwrap();
        let mut hello_text = String::new();
        hello.read_to_string(&mut hello_text).unwrap();
        assert_eq!(hello_text, "formatted via high-level\n");
    }

    let meta = fs.metadata("/a/big.bin").unwrap();
    assert_eq!(meta.size, big.len() as u64);

    {
        let mut deep = fs.open("/a/b/c/deep.txt", OpenFlags::READ).unwrap();
        let mut deep_text = String::new();
        deep.read_to_string(&mut deep_text).unwrap();
        assert_eq!(deep_text, "deep nested\n");
    }

    drop(fs);
    store.close().await.unwrap();
}
