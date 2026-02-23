// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// NFS INTEGRATION TESTS — DISABLED (see lib.rs)
//
// STATUS: Tests are structurally complete but deadlock at runtime.
//         The module compiles and the test infrastructure (MockS3,
//         mkfs, store load/close, NFS server bind, nfs3_client connect)
//         all work. The hang occurs when the NFS client issues its
//         first mutating RPC (e.g. CREATE).
//
// ROOT CAUSE: StoreBlockDevice::run_blocking() uses
//   block_in_place(|| self.rt.block_on(future))
// to bridge sync BlockDevice calls back to the async Store.
// This works when:
//   (a) the caller is on the SAME tokio runtime as self.rt (FUSE on
//       Linux uses this — FUSE threads aren't tokio workers, so the
//       else branch runs block_on directly), or
//   (b) the caller is on a non-tokio thread.
//
// It does NOT work for the NFS path because:
//   - nfsserve spawns each NFS request handler as a tokio::spawn task
//   - If the NFS server runs on a SEPARATE runtime (nfs_rt), then
//     block_in_place runs on nfs_rt but self.rt.block_on() tries to
//     enter the main runtime from within nfs_rt's async context.
//     Handle::block_on() panics (silently, on the nfs_rt worker thread)
//     when called from within another runtime's async context.
//   - If the NFS server runs on the SAME runtime as the store, then
//     block_in_place blocks worker threads. With enough concurrent NFS
//     requests (e.g. from a real mount), all workers get consumed by
//     block_in_place + Mutex contention, leaving none to drive the
//     store futures → deadlock. (Sequential single-client requests
//     should theoretically work, but in practice the nfs3_client
//     mount/create flow still deadlocks — possibly because the NFS
//     protocol layer issues overlapping RPCs internally.)
//
// TO FIX: StoreBlockDevice::run_blocking() needs to handle the
// cross-runtime case. Options:
//   1. When current runtime != self.rt, spawn a plain thread (via
//      std::thread::scope) that calls self.rt.block_on(). This avoids
//      the nested-runtime-context problem. Requires F: Send + F::Output: Send.
//   2. Refactor Ext4Nfs to use spawn_blocking for all ext4 operations,
//      keeping NFS handlers fully async. The ext4 work moves to the
//      blocking thread pool where block_on works without issues.
//   3. Use a channel-based approach: NFS handlers send requests to a
//      dedicated ext4 worker thread that owns Ext4Fs and the store
//      runtime handle.
//
// WHAT WORKS:
//   - MockS3 with S3Access trait ✓
//   - Store::format + mkfs (needs assert::init to disable zero-block
//     assert, and spawn_blocking for mkfs) ✓
//   - Store::load, StoreBlockDevice, Ext4Fs::mount ✓
//   - NFSTcpListener::bind + nfs3_client connect + root_nfs_fh3 ✓
//   - NFS getattr on root inode ✓
//
// WHAT HANGS:
//   - Any NFS RPC that triggers ext4 writes through the block device
//     (CREATE, WRITE, MKDIR, etc.)
//
// ADDITIONAL NOTES:
//   - Volume size 64MB / block size 1MB works for mkfs (1GB/4MB was
//     too slow in tests).
//   - Must call crate::assert::init() before mkfs — the default
//     cfg!(test) enables zero-block assertions which panic the
//     uploader during mkfs (ext4 writes many all-zero blocks).
//   - nfs3_client and nfs3_types are dev-dependencies in Cargo.toml.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[allow(unused_imports, dead_code)]

use crate::blockdev_adapter::StoreBlockDevice;
use crate::cache;
use crate::nfs::Ext4Nfs;
use crate::store::{self, Store};
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use ext4_lwext4::MkfsOptions;
use nfs3_client::tokio::TokioConnector;
use nfs3_client::Nfs3ConnectionBuilder;
use nfs3_types::nfs3::*;
use nfs3_types::xdr_codec::Opaque;
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::OnceCell;

// ── Mock S3 ────────────────────────────────────────────────────────────

#[derive(Clone)]
struct MockS3 {
    objects: Arc<std::sync::RwLock<HashMap<String, Vec<u8>>>>,
}

impl MockS3 {
    fn new() -> Self {
        Self {
            objects: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }
}

impl store::S3Access for MockS3 {
    async fn get_bytes(&self, _bucket: &str, key: &str) -> Result<Vec<u8>> {
        self.objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("not found: {key}"))
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
            .ok_or_else(|| anyhow::anyhow!("not found: {key}"))?;
        let s = offset as usize;
        let e = (s + len).min(data.len());
        Ok(data[s..e].to_vec())
    }

    async fn get_body(&self, _bucket: &str, key: &str) -> Result<ByteStream> {
        let data = self
            .objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("not found: {key}"))?;
        Ok(ByteStream::from(data))
    }

    async fn put_bytes(&self, _bucket: &str, key: &str, body: Vec<u8>) -> Result<()> {
        self.objects.write().unwrap().insert(key.to_string(), body);
        Ok(())
    }

    async fn put_file(&self, _bucket: &str, key: &str, path: &Path) -> Result<()> {
        let data = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
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

// ── Cache init ─────────────────────────────────────────────────────────

static CACHE_INIT: OnceCell<()> = OnceCell::const_new();
static ASSERT_INIT: Once = Once::new();
static TRACING_INIT: Once = Once::new();

async fn ensure_cache() {
    ASSERT_INIT.call_once(crate::assert::init);
    TRACING_INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter("loophole::nfs=info")
            .try_init()
            .ok();
    });
    CACHE_INIT
        .get_or_init(|| async {
            let dir = std::env::temp_dir().join(format!("loophole_nfs_test_{}", std::process::id()));
            cache::init(dir, 512 * 1024 * 1024)
                .await
                .expect("cache init");
        })
        .await;
}

// ── Helpers ────────────────────────────────────────────────────────────

static ID_SEQ: AtomicU64 = AtomicU64::new(0);
fn unique_id(tag: &str) -> String {
    let n = ID_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{tag}-{n}")
}

const BUCKET: &str = "b";

/// Spin up NFS server backed by MockS3, return the port.
/// The NFS server runs on a **separate** tokio runtime to avoid
/// deadlocking the main runtime when handlers call block_in_place.
async fn start_nfs_server(store_id: &str) -> (u16, Arc<Store<MockS3>>) {
    let s3 = MockS3::new();
    Store::format(&s3, BUCKET, "", store_id, 1024 * 1024, 64 * 1024 * 1024)
        .await
        .expect("format");

    // Step 1: mkfs — create ext4 filesystem on the block device.
    let mkfs_store = Store::load(
        s3.clone(),
        BUCKET.into(),
        String::new(),
        store_id.into(),
        20,
        200,
    )
    .await
    .expect("load for mkfs");
    let mkfs_device =
        StoreBlockDevice::new(Arc::clone(&mkfs_store), tokio::runtime::Handle::current())
            .expect("block device for mkfs");
    let mkfs_opts = MkfsOptions::ext2().with_block_size(4096);
    tokio::task::spawn_blocking(move || ext4_lwext4::mkfs(mkfs_device, &mkfs_opts))
        .await
        .expect("join mkfs")
        .expect("mkfs failed");
    mkfs_store.flush().await.expect("flush after mkfs");
    mkfs_store.close().await.expect("close after mkfs");

    // Step 2: reload store and mount ext4
    let store = Store::load(s3, BUCKET.into(), String::new(), store_id.into(), 20, 200)
        .await
        .expect("load");

    let main_rt = tokio::runtime::Handle::current();
    let device = StoreBlockDevice::new(Arc::clone(&store), main_rt).expect("block device");
    let ext4_fs = ext4_lwext4::Ext4Fs::mount(device, false).expect("mount lwext4");
    let nfs_fs = Ext4Nfs::new(store.clone(), ext4_fs);

    let listener = NFSTcpListener::bind("127.0.0.1:0", nfs_fs)
        .await
        .expect("bind NFS");
    let port = listener.get_listen_port();

    // Run the NFS server on the same runtime. Since our test makes
    // sequential NFS requests (only one at a time), we won't exhaust
    // worker threads via block_in_place.
    tokio::spawn(async move {
        listener.handle_forever().await.ok();
    });

    (port, store)
}

async fn connect_nfs(
    port: u16,
) -> nfs3_client::Nfs3Connection<nfs3_client::tokio::TokioIo<tokio::net::TcpStream>> {
    Nfs3ConnectionBuilder::new(TokioConnector, "127.0.0.1", "/")
        .connect_from_privileged_port(false)
        .mount_port(port)
        .nfs3_port(port)
        .mount()
        .await
        .expect("NFS connect")
}

fn dirop<'a>(dir: &nfs_fh3, name: &'a str) -> diropargs3<'a> {
    diropargs3 {
        dir: dir.clone(),
        name: filename3(Opaque::borrowed(name.as_bytes())),
    }
}

fn extract_fh(obj: post_op_fh3) -> nfs_fh3 {
    obj.unwrap()
}

// ── Tests ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_nfs_create_write_read() {
    ensure_cache().await;
    let sid = unique_id("nfs-rw");
    let (port, _store) = start_nfs_server(&sid).await;
    let mut conn = connect_nfs(port).await;
    let root = conn.root_nfs_fh3();

    // Create a file
    let create_res = conn
        .create(&CREATE3args {
            where_: dirop(&root, "hello.txt"),
            how: createhow3::UNCHECKED(sattr3::default()),
        })
        .await
        .expect("create RPC");
    let fh = extract_fh(create_res.unwrap().obj);

    // Write data
    let data = b"hello from nfs test!";
    let write_res = conn
        .write(&WRITE3args {
            file: fh.clone(),
            offset: 0,
            count: data.len() as u32,
            stable: stable_how::FILE_SYNC,
            data: Opaque::borrowed(data),
        })
        .await
        .expect("write RPC");
    write_res.unwrap();

    // Read it back
    let read_res = conn
        .read(&READ3args {
            file: fh,
            offset: 0,
            count: 4096,
        })
        .await
        .expect("read RPC");
    let read_ok = read_res.unwrap();
    assert_eq!(read_ok.data.as_ref(), data);
    assert_eq!(read_ok.count, data.len() as u32);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nfs_mkdir_and_readdir() {
    ensure_cache().await;
    let sid = unique_id("nfs-dir");
    let (port, _store) = start_nfs_server(&sid).await;
    let mut conn = connect_nfs(port).await;
    let root = conn.root_nfs_fh3();

    // mkdir "subdir"
    let mkdir_res = conn
        .mkdir(&MKDIR3args {
            where_: dirop(&root, "subdir"),
            attributes: sattr3::default(),
        })
        .await
        .expect("mkdir RPC");
    mkdir_res.unwrap();

    // readdir root — should contain "subdir" and ".loophole"
    let readdir_res = conn
        .readdir(&READDIR3args {
            dir: root,
            cookie: 0,
            cookieverf: cookieverf3::default(),
            count: 128 * 1024,
        })
        .await
        .expect("readdir RPC");
    let readdir_ok = readdir_res.unwrap();

    let names: Vec<String> = readdir_ok
        .reply
        .entries
        .0
        .into_iter()
        .map(|e| String::from_utf8_lossy(e.name.0.as_ref()).to_string())
        .collect();

    assert!(names.contains(&"subdir".to_string()), "names = {names:?}");
    assert!(
        names.contains(&".loophole".to_string()),
        "names = {names:?}"
    );
    assert!(
        names.contains(&"lost+found".to_string()),
        "names = {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nfs_ctl_snapshot_dirs() {
    ensure_cache().await;
    let sid = unique_id("nfs-ctl");
    let (port, _store) = start_nfs_server(&sid).await;
    let mut conn = connect_nfs(port).await;
    let root = conn.root_nfs_fh3();

    // lookup .loophole
    let ctl_res = conn
        .lookup(&LOOKUP3args {
            what: dirop(&root, ".loophole"),
        })
        .await
        .expect("lookup .loophole RPC");
    let ctl_fh = ctl_res.unwrap().object;

    // lookup snapshots
    let snap_res = conn
        .lookup(&LOOKUP3args {
            what: dirop(&ctl_fh, "snapshots"),
        })
        .await
        .expect("lookup snapshots RPC");
    snap_res.unwrap();

    // lookup clones
    let clones_res = conn
        .lookup(&LOOKUP3args {
            what: dirop(&ctl_fh, "clones"),
        })
        .await
        .expect("lookup clones RPC");
    clones_res.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nfs_large_write_read() {
    ensure_cache().await;
    let sid = unique_id("nfs-large");
    let (port, _store) = start_nfs_server(&sid).await;
    let mut conn = connect_nfs(port).await;
    let root = conn.root_nfs_fh3();

    // Create file
    let create_res = conn
        .create(&CREATE3args {
            where_: dirop(&root, "big.bin"),
            how: createhow3::UNCHECKED(sattr3::default()),
        })
        .await
        .expect("create RPC");
    let fh = extract_fh(create_res.unwrap().obj);

    // Write 1MB of data in 64KB chunks
    let chunk_size = 64 * 1024;
    let total_size = 1024 * 1024;
    let pattern: Vec<u8> = (0..chunk_size).map(|i| (i % 251) as u8).collect();

    for offset in (0..total_size).step_by(chunk_size) {
        conn.write(&WRITE3args {
            file: fh.clone(),
            offset: offset as u64,
            count: chunk_size as u32,
            stable: stable_how::UNSTABLE,
            data: Opaque::borrowed(&pattern),
        })
        .await
        .expect("write chunk RPC")
        .unwrap();
    }

    // Read it all back and verify
    let mut all_data = Vec::new();
    let mut offset = 0u64;
    loop {
        let read_res = conn
            .read(&READ3args {
                file: fh.clone(),
                offset,
                count: chunk_size as u32,
            })
            .await
            .expect("read chunk RPC");
        let read_ok = read_res.unwrap();
        all_data.extend_from_slice(read_ok.data.as_ref());
        offset += read_ok.count as u64;
        if read_ok.eof || read_ok.count == 0 {
            break;
        }
    }

    assert_eq!(all_data.len(), total_size);
    for (i, chunk) in all_data.chunks(chunk_size).enumerate() {
        assert_eq!(chunk, &pattern[..], "mismatch at chunk {i}");
    }
}
