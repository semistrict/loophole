use super::*;
use tempfile::TempDir;

struct TestLocker(dashmap::DashMap<u64, Arc<AsyncMutex<()>>>);

impl BlockLocker for TestLocker {
    fn block_lock(&self, block_idx: u64) -> Arc<AsyncMutex<()>> {
        Arc::clone(
            &*self
                .0
                .entry(block_idx)
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }
}

fn make_locker() -> Arc<dyn BlockLocker> {
    Arc::new(TestLocker(dashmap::DashMap::new()))
}

// ---------------------------------------------------------------------------
// pread
// ---------------------------------------------------------------------------

/// pread should return exactly the bytes at the given offset.
#[test]
fn pread_returns_correct_bytes() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("block");
    std::fs::write(&path, b"hello world").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let data = pread(&file, 6, 5).unwrap();
    assert_eq!(data, b"world");
}

/// pread must error when fewer bytes are available than requested.
/// Previously it silently returned a short buffer, masking data corruption.
#[test]
fn pread_errors_on_short_read() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("block");
    std::fs::write(&path, b"hello").unwrap(); // only 5 bytes
    let file = std::fs::File::open(&path).unwrap();
    let err = pread(&file, 0, 10).unwrap_err(); // request 10
    assert!(
        err.to_string().contains("short read"),
        "expected 'short read' in error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// recover()
// ---------------------------------------------------------------------------

/// Stale .pending files from aborted background downloads must be deleted
/// and must NOT be re-queued for upload.
#[test]
fn recover_removes_stale_pending_files() {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), 10 * 1024 * 1024);
    cache.init_store("s1", make_locker()).unwrap();
    let store_dir = cache.store_dir("s1");

    std::fs::write(store_dir.join("0000000000000001.pending"), b"partial").unwrap();

    let dirty = cache.recover("s1").unwrap();

    assert!(
        dirty.is_empty(),
        "pending block should not be queued: {dirty:?}"
    );
    assert!(
        !store_dir.join("0000000000000001.pending").exists(),
        ".pending file should have been deleted"
    );
}

/// When both .uploading and .dirty exist for the same block (crash mid-upload),
/// recover() must return that block exactly once — not twice.
#[test]
fn recover_deduplicates_uploading_and_dirty_for_same_block() {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), 10 * 1024 * 1024);
    cache.init_store("s1", make_locker()).unwrap();
    let store_dir = cache.store_dir("s1");

    let block_idx = 0x42u64;
    let block_name = format!("{:016x}", block_idx);

    // Simulate crash mid-upload: block file + .uploading + .dirty all present.
    std::fs::write(store_dir.join(&block_name), b"block data").unwrap();
    std::fs::write(store_dir.join(format!("{block_name}.uploading")), b"copy").unwrap();
    std::fs::write(store_dir.join(format!("{block_name}.dirty")), b"").unwrap();

    let dirty = cache.recover("s1").unwrap();

    assert_eq!(
        dirty.len(),
        1,
        "block 0x42 should appear exactly once, got: {dirty:?}"
    );
    assert_eq!(dirty[0], block_idx);
}

/// .uploading without a matching block file must not be re-queued
/// (the upload copy was partial; there is nothing to re-upload).
#[test]
fn recover_ignores_uploading_without_block_file() {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), 10 * 1024 * 1024);
    cache.init_store("s1", make_locker()).unwrap();
    let store_dir = cache.store_dir("s1");

    // Only the .uploading copy — no block file.
    std::fs::write(
        store_dir.join("0000000000000007.uploading"),
        b"partial copy",
    )
    .unwrap();

    let dirty = cache.recover("s1").unwrap();

    assert!(
        dirty.is_empty(),
        "orphan .uploading should not be queued: {dirty:?}"
    );
}

// ---------------------------------------------------------------------------
// lru_insert — used_bytes accounting
// ---------------------------------------------------------------------------

/// Inserting a new block should add its size to used_bytes.
#[test]
fn lru_insert_tracks_new_entry_size() {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), 10 * 1024 * 1024);
    cache.lru_insert("s1", 0, make_locker(), 1024);
    assert_eq!(cache.used_bytes_snapshot(), 1024);
}

/// Overwriting an existing LRU entry with a larger block must add only
/// the delta — not the full new size again.
/// Bug: previously used_bytes accumulated both old and new size.
#[test]
fn lru_insert_delta_on_grow() {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), 10 * 1024 * 1024);
    cache.lru_insert("s1", 0, make_locker(), 1000);
    cache.lru_insert("s1", 0, make_locker(), 2000);
    assert_eq!(
        cache.used_bytes_snapshot(),
        2000,
        "should be 2000, not 3000 (double-count)"
    );
}

/// Overwriting with a smaller block must subtract the difference.
#[test]
fn lru_insert_delta_on_shrink() {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), 10 * 1024 * 1024);
    cache.lru_insert("s1", 0, make_locker(), 2000);
    cache.lru_insert("s1", 0, make_locker(), 500);
    assert_eq!(cache.used_bytes_snapshot(), 500, "should be 500, not 2500");
}

/// Multiple distinct blocks must each contribute their size.
#[test]
fn lru_insert_accumulates_multiple_blocks() {
    let tmp = TempDir::new().unwrap();
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), 10 * 1024 * 1024);
    cache.lru_insert("s1", 0, make_locker(), 100);
    cache.lru_insert("s1", 1, make_locker(), 200);
    cache.lru_insert("s1", 2, make_locker(), 300);
    assert_eq!(cache.used_bytes_snapshot(), 600);
}

// ---------------------------------------------------------------------------
// evict_if_needed
// ---------------------------------------------------------------------------

fn write_block_file(cache: &DiskCache, store_id: &str, block_idx: u64, size: usize) {
    let path = cache.block_path(store_id, block_idx);
    std::fs::write(&path, vec![0u8; size]).unwrap();
}

/// A clean block (no .dirty, no .uploading) must be evicted when over limit.
#[test]
fn evict_removes_clean_block_when_over_limit() {
    let tmp = TempDir::new().unwrap();
    let max: u64 = 512;
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), max);
    let locks = make_locker();
    cache.init_store("s1", Arc::clone(&locks)).unwrap();

    let size = (max + 1) as usize;
    write_block_file(&cache, "s1", 0, size);
    cache.lru_insert("s1", 0, Arc::clone(&locks), size as u64);

    cache.evict_if_needed();

    assert!(
        !cache.block_path("s1", 0).exists(),
        "clean block should have been evicted"
    );
    assert_eq!(cache.used_bytes_snapshot(), 0);
}

/// A block with a .dirty marker must never be evicted (it has un-uploaded data).
#[test]
fn evict_skips_block_with_dirty_marker() {
    let tmp = TempDir::new().unwrap();
    let max: u64 = 512;
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), max);
    let locks = make_locker();
    cache.init_store("s1", Arc::clone(&locks)).unwrap();

    let size = (max + 1) as usize;
    write_block_file(&cache, "s1", 0, size);
    std::fs::write(cache.dirty_path("s1", 0), b"").unwrap();
    cache.lru_insert("s1", 0, Arc::clone(&locks), size as u64);

    cache.evict_if_needed();

    assert!(
        cache.block_path("s1", 0).exists(),
        "dirty block must NOT be evicted"
    );
}

/// A block with a .uploading marker must never be evicted (upload in flight).
#[test]
fn evict_skips_block_with_uploading_marker() {
    let tmp = TempDir::new().unwrap();
    let max: u64 = 512;
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), max);
    let locks = make_locker();
    cache.init_store("s1", Arc::clone(&locks)).unwrap();

    let size = (max + 1) as usize;
    write_block_file(&cache, "s1", 0, size);
    std::fs::write(cache.uploading_path("s1", 0), b"copy").unwrap();
    cache.lru_insert("s1", 0, Arc::clone(&locks), size as u64);

    cache.evict_if_needed();

    assert!(
        cache.block_path("s1", 0).exists(),
        "uploading block must NOT be evicted"
    );
}

/// When multiple blocks exist, only the LRU one(s) are evicted.
/// With 3 blocks of 100 bytes and max=290, target=261. Evicting the oldest
/// block (100 bytes) brings used to 200 which is under the target, so only
/// block 0 (LRU) is removed and blocks 1 and 2 survive.
#[test]
fn evict_removes_lru_block_first() {
    let tmp = TempDir::new().unwrap();
    let block_size: u64 = 100;
    // max=290: 3 blocks (300) triggers eviction; target=261; one eviction suffices.
    let max: u64 = 290;
    let cache = DiskCache::new_for_test(tmp.path().to_path_buf(), max);
    let locks = make_locker();
    cache.init_store("s1", Arc::clone(&locks)).unwrap();

    // Insert in order: 0 is LRU, 2 is MRU.
    write_block_file(&cache, "s1", 0, block_size as usize);
    cache.lru_insert("s1", 0, Arc::clone(&locks), block_size);

    write_block_file(&cache, "s1", 1, block_size as usize);
    cache.lru_insert("s1", 1, Arc::clone(&locks), block_size);

    write_block_file(&cache, "s1", 2, block_size as usize);
    cache.lru_insert("s1", 2, Arc::clone(&locks), block_size);

    cache.evict_if_needed();

    assert!(
        !cache.block_path("s1", 0).exists(),
        "block 0 (LRU) should be evicted"
    );
    assert!(cache.block_path("s1", 1).exists(), "block 1 should survive");
    assert!(
        cache.block_path("s1", 2).exists(),
        "block 2 (MRU) should survive"
    );
}
