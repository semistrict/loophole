use super::*;
use crate::s3::State;
use crate::test_helpers::*;
use std::sync::Arc;

async fn make_vm(s3: MockS3, volume_name: &str, store_id: &str) -> Arc<VolumeManager<MockS3>> {
    let store = load_store(s3.clone(), store_id).await;
    let vm = Arc::new(VolumeManager::new(
        s3,
        BUCKET.to_string(),
        "".to_string(),
        4,
        4,
    ));
    vm.add_volume(volume_name.to_string(), store);
    vm
}

// =======================================================================
// Snapshot tests
// =======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_volume_stays_writable() {
    let sid = unique_id("snap-wr");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"AAAA").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    store.write(100, b"BBBB").await.unwrap();

    let got = store.read(0, 4).await.unwrap();
    assert_eq!(got, b"AAAA");

    let got = store.read(100, 4).await.unwrap();
    assert_eq!(got, b"BBBB");

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn snapshot_continuation_inherits_parent_data() {
    let sid = unique_id("snap-inherit");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"BEFORE").await.unwrap();
    store.flush().await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    store.write(100, b"AFTER").await.unwrap();

    let got = store.read(0, 6).await.unwrap();
    assert_eq!(got, b"BEFORE");

    let got = store.read(100, 5).await.unwrap();
    assert_eq!(got, b"AFTER");

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn multiple_snapshots_in_sequence() {
    let sid = unique_id("snap-seq");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"A").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    store.write(64, b"B").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap2").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    store.write(128, b"C").await.unwrap();

    assert_eq!(store.read(0, 1).await.unwrap(), b"A");
    assert_eq!(store.read(64, 1).await.unwrap(), b"B");
    assert_eq!(store.read(128, 1).await.unwrap(), b"C");

    vm.close_all().await.unwrap();
}

// =======================================================================
// Clone tests
// =======================================================================

#[tokio::test]
async fn clone_volume_stays_writable() {
    let sid = unique_id("clone-wr");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"AAAA").await.unwrap();
    drop(store);

    vm.clone_volume("mydata", "branch").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    store.write(100, b"BBBB").await.unwrap();

    let got = store.read(0, 4).await.unwrap();
    assert_eq!(got, b"AAAA");
    let got = store.read(100, 4).await.unwrap();
    assert_eq!(got, b"BBBB");

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn clone_branch_has_pre_clone_data() {
    let sid = unique_id("clone-data");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3.clone(), "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"BEFORE").await.unwrap();
    store.flush().await.unwrap();
    drop(store);

    vm.clone_volume("mydata", "branch").await.unwrap();

    // Load the clone store from S3 directly to verify it has the data.
    let clone_ref = crate::names::read_volume_ref(&s3, BUCKET, "", "branch")
        .await
        .unwrap();
    let clone_store = load_store(s3, &clone_ref.uuid).await;
    let got = clone_store.read(0, 6).await.unwrap();
    assert_eq!(got, b"BEFORE");
    clone_store.close().await.unwrap();

    vm.close_all().await.unwrap();
}

// =======================================================================
// Ancestry tests (child reads parent blocks, cow isolation)
// =======================================================================

#[tokio::test]
async fn child_reads_parent_blocks_after_snapshot() {
    let sid = unique_id("anc-read");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"inherited").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    let got = store.read(0, 9).await.unwrap();
    assert_eq!(got, b"inherited");

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn child_writes_do_not_affect_parent() {
    let sid = unique_id("anc-cow");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3.clone(), "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"original").await.unwrap();
    store.flush().await.unwrap();

    let parent_store_id = store.id.clone();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"modified").await.unwrap();
    store.flush().await.unwrap();

    // Parent's S3 block should still have the original data.
    let parent_block_key = format!("stores/{parent_store_id}/0000000000000000");
    let parent_s3_data = s3.get_object(&parent_block_key).unwrap();
    assert_eq!(&parent_s3_data[..8], b"original");

    vm.close_all().await.unwrap();
}

// =======================================================================
// Zero-block tests with ancestry (tombstones)
// =======================================================================

#[tokio::test]
async fn zero_block_with_ancestor_creates_tombstone() {
    let sid = unique_id("zb-anc");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3.clone(), "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"parent data!").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();

    let got = store.read(0, 12).await.unwrap();
    assert_eq!(got, b"parent data!");

    // Writing a full block of zeros triggers do_zero_block internally
    store.write(0, &[0u8; 64]).await.unwrap();
    store.flush().await.unwrap();

    let child_block_key = format!("stores/{}/0000000000000000", store.id);
    let tombstone = s3.get_object(&child_block_key).expect("tombstone should exist");
    assert_eq!(tombstone.len(), 0, "tombstone should be 0 bytes");

    let got = store.read(0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn write_full_zeros_triggers_tombstone_with_ancestor() {
    let sid = unique_id("zb-fullzero");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3.clone(), "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"some data").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    store.write(0, &[0u8; 64]).await.unwrap();
    store.flush().await.unwrap();

    let child_block_key = format!("stores/{}/0000000000000000", store.id);
    let obj = s3.get_object(&child_block_key).expect("tombstone should exist");
    assert_eq!(obj.len(), 0, "should be tombstone, not full zero block");

    let got = store.read(0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn tombstone_survives_reload() {
    let sid = unique_id("zb-reload");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3.clone(), "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"parent data").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    let child_id = store.id.clone();
    store.write(0, &[0u8; 64]).await.unwrap();
    store.flush().await.unwrap();
    drop(store);

    vm.close_all().await.unwrap();
    let child2 = load_store(s3, &child_id).await;

    assert!(child2.zero_blocks.contains(&0), "zero_blocks should contain block 0 after reload");

    let got = child2.read(0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    child2.close().await.unwrap();
}

#[tokio::test]
async fn zero_range_full_block_becomes_tombstone_with_ancestor() {
    let sid = unique_id("zr-full");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"parent data here!").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    // Writing a full block of zeros at offset 0 triggers the tombstone path
    store.write(0, &[0u8; 64]).await.unwrap();

    assert!(store.zero_blocks.contains(&0), "full-block zero write should tombstone");
    let got = store.read(0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn zero_block_then_immediate_write_survives_flush() {
    let sid = unique_id("zb-race");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3.clone(), "mydata", &sid).await;

    let store = vm.get_store("mydata").unwrap();
    store.write(0, b"parent-data-here").await.unwrap();
    drop(store);

    vm.snapshot("mydata", "snap1").await.unwrap();

    let store = vm.get_store("mydata").unwrap();
    let child_id = store.id.clone();

    store.write(0, &[0u8; 64]).await.unwrap();
    assert!(store.zero_blocks.contains(&0));

    store.write(0, b"new-real-data!!!").await.unwrap();
    assert!(!store.zero_blocks.contains(&0), "write should clear zero_blocks");

    store.flush().await.unwrap();

    let child_block_key = format!("stores/{child_id}/0000000000000000");
    let obj = s3.get_object(&child_block_key).expect("block should exist in S3");
    assert!(!obj.is_empty(), "block should not be a tombstone");
    assert_eq!(&obj[..16], b"new-real-data!!!");

    let child2 = load_store(s3, &child_id).await;
    let got = child2.read(0, 16).await.unwrap();
    assert_eq!(got, b"new-real-data!!!");
    child2.close().await.unwrap();

    vm.close_all().await.unwrap();
}

// =======================================================================
// Ctl tests
// =======================================================================

#[tokio::test]
async fn ctl_snapshot() {
    let sid = unique_id("ctl-snap");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;
    let ctl = crate::ctl::Ctl::new(Arc::clone(&vm));

    use crate::inode::SNAPSHOTS_DIR_INO;
    let snap_entries = ctl.readdir(SNAPSHOTS_DIR_INO).expect("snapshots dir");
    let snap_vol_ino = snap_entries[0].ino.to_virtual().expect("should be virtual");

    let result = ctl.create(snap_vol_ino, "snap1").await;
    assert!(result.is_some());
    let (ino, _size) = result.unwrap().expect("snapshot should succeed");

    let virt = ino.to_virtual().expect("should be virtual");
    let (data, _eof) = ctl.read(virt, 0, 4096).expect("virtual file should be readable");
    let status: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(status["status"], "complete");

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn ctl_clone() {
    let sid = unique_id("ctl-clone");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3.clone(), "mydata", &sid).await;
    let ctl = crate::ctl::Ctl::new(Arc::clone(&vm));

    use crate::inode::CLONES_DIR_INO;
    let clone_entries = ctl.readdir(CLONES_DIR_INO).expect("clones dir");
    let clone_vol_ino = clone_entries[0].ino.to_virtual().expect("should be virtual");

    let result = ctl.create(clone_vol_ino, "branch").await;
    assert!(result.is_some());
    let (ino, _size) = result.unwrap().expect("clone should succeed");

    let virt = ino.to_virtual().expect("should be virtual");
    let (data, _eof) = ctl.read(virt, 0, 4096).expect("virtual file should be readable");
    let status: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(status["status"], "complete");

    let parent_key = format!("stores/{sid}/state.json");
    let parent_bytes = s3.get_object(&parent_key).expect("parent state should exist");
    let parent_state: State = serde_json::from_slice(&parent_bytes).unwrap();
    assert!(parent_state.frozen_at.is_some(), "parent should be frozen");
    assert_eq!(parent_state.children.len(), 2, "clone should create two children");

    vm.close_all().await.unwrap();
}

#[tokio::test]
async fn ctl_snapshot_on_frozen_store_fails() {
    let sid = unique_id("ctl-frozen");
    let state = State {
        parent_id: None,
        block_size: 64,
        volume_size: 1024,
        frozen_at: Some("2025-01-01T00:00:00Z".to_string()),
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let vm = make_vm(s3, "mydata", &sid).await;
    let ctl = crate::ctl::Ctl::new(Arc::clone(&vm));

    use crate::inode::SNAPSHOTS_DIR_INO;
    let snap_entries = ctl.readdir(SNAPSHOTS_DIR_INO).expect("snapshots dir");
    let snap_vol_ino = snap_entries[0].ino.to_virtual().expect("should be virtual");

    let result = ctl.create(snap_vol_ino, "another-snap").await;
    assert!(result.is_some());
    let (ino, _size) = result.unwrap().expect("create returns Ok even on error status");

    let virt = ino.to_virtual().expect("should be virtual");
    let (data, _eof) = ctl.read(virt, 0, 4096).expect("virtual file should be readable");
    let status: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(status["status"], "error");
    assert!(status["error"].is_string());

    vm.close_all().await.unwrap();
}
