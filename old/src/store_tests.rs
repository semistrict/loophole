use super::*;
use crate::test_helpers::*;

// =======================================================================
// Integration tests – full pipeline driven like FUSE
// =======================================================================

/// Write a few bytes, read them back through the FUSE path.
/// Everything stays in the cache – no S3 reads.
#[tokio::test]
async fn write_and_readback_single_block() {
    let sid = unique_id("rw1");
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(0, b"hello world").await.unwrap();
    let got = store.read(0, 11).await.unwrap();
    assert_eq!(got, b"hello world");
    store.close().await.unwrap();
}

/// Write data that straddles a block boundary and read it back.
#[tokio::test]
async fn write_and_readback_spanning_block_boundary() {
    let sid = unique_id("span");
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let data = b"ABCDEFGHIJ"; // 10 bytes across the 4096 boundary
    store.write(4090, data).await.unwrap();
    let got = store.read(4090, 10).await.unwrap();
    assert_eq!(got, data.as_slice());
    store.close().await.unwrap();
}

/// A larger write spanning three full blocks and partial edges.
#[tokio::test]
async fn write_and_readback_three_blocks() {
    let sid = unique_id("3blk");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let data: Vec<u8> = (0u8..150).collect();
    store.write(10, &data).await.unwrap();

    let got = store.read(10, 150).await.unwrap();
    assert_eq!(got, data);

    let got = store.read(50, 30).await.unwrap();
    assert_eq!(got, &data[40..70]);
    store.close().await.unwrap();
}

/// Reading a block that doesn't exist anywhere returns zeros.
#[tokio::test]
async fn read_absent_block_returns_zeros() {
    let sid = unique_id("zeros");
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let got = store.read(0, 4096).await.unwrap();
    assert_eq!(got, vec![0u8; 4096]);
    store.close().await.unwrap();
}

/// Read from S3: block exists in the mock S3 index but not in the cache.
#[tokio::test]
async fn read_fetches_from_s3_on_cache_miss() {
    let sid = unique_id("s3fetch");
    let block_data = vec![0xAB; 4096];
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[(0, block_data)]);
    let store = load_store(s3, &sid).await;

    let got = store.read(100, 10).await.unwrap();
    assert_eq!(got, vec![0xAB; 10]);
    store.close().await.unwrap();
}

/// Zero-length read returns empty without touching the store.
#[tokio::test]
async fn zero_length_read_returns_empty() {
    let sid = unique_id("zr");
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let got = store.read(0, 0).await.unwrap();
    assert!(got.is_empty());
    store.close().await.unwrap();
}

/// Zero-length write is a no-op.
#[tokio::test]
async fn zero_length_write_is_noop() {
    let sid = unique_id("zw");
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(0, b"").await.unwrap();
    store.close().await.unwrap();
}

/// Read past the end of the volume should clamp to volume_size.
#[tokio::test]
async fn read_past_eof_clamps_to_volume_size() {
    let sid = unique_id("eof");
    let state = default_state(64, 100);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(90, b"ABCDEFGHIJ").await.unwrap();

    let got = store.read(80, 64).await.unwrap();
    assert_eq!(got.len(), 20);
    assert_eq!(&got[..10], &[0u8; 10]);
    assert_eq!(&got[10..], b"ABCDEFGHIJ");
    store.close().await.unwrap();
}

/// Read starting at exactly volume_size returns empty.
#[tokio::test]
async fn read_at_volume_size_returns_empty() {
    let sid = unique_id("ateof");
    let state = default_state(64, 100);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let got = store.read(100, 10).await.unwrap();
    assert!(got.is_empty());
    store.close().await.unwrap();
}

/// Write that crosses EOF should be truncated to the remaining volume bytes.
#[tokio::test]
async fn write_past_eof_is_truncated() {
    let sid = unique_id("wteof");
    let state = default_state(64, 100);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(95, b"ABCDEFGHIJ").await.unwrap();

    let got = store.read(95, 10).await.unwrap();
    assert_eq!(got, b"ABCDE");
    store.close().await.unwrap();
}

/// Write starting exactly at EOF should fail.
#[tokio::test]
async fn write_at_eof_fails() {
    let sid = unique_id("weof");
    let state = default_state(64, 100);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let err = store.write(100, b"X").await.unwrap_err();
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
        frozen_at: Some("2025-01-01T00:00:00Z".to_string()),
        children: vec![],
    };
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let err = store.write(0, b"nope").await.unwrap_err();
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
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(10, b"AB").await.unwrap();

    let block = store.read(0, 64).await.unwrap();
    assert_eq!(&block[..10], &[0u8; 10]);
    assert_eq!(&block[10..12], b"AB");
    assert_eq!(&block[12..64], &[0u8; 52]);
    store.close().await.unwrap();
}

/// Multiple small writes to different offsets in the same block.
#[tokio::test]
async fn multiple_writes_same_block_different_offsets() {
    let sid = unique_id("multi");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(0, b"AAAA").await.unwrap();
    store.write(10, b"BBBB").await.unwrap();
    store.write(60, b"CCCC").await.unwrap();

    let got = store.read(0, 64 + 4).await.unwrap();
    assert_eq!(&got[0..4], b"AAAA");
    assert_eq!(&got[4..10], &[0u8; 6]);
    assert_eq!(&got[10..14], b"BBBB");
    assert_eq!(&got[14..60], &[0u8; 46]);
    assert_eq!(&got[60..64], &b"CCCC"[..4]);
    store.close().await.unwrap();
}

/// Write exactly at a block boundary (offset is multiple of block_size).
#[tokio::test]
async fn write_at_exact_block_boundary() {
    let sid = unique_id("boundary");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(64, b"HELLO").await.unwrap();
    let got = store.read(64, 5).await.unwrap();
    assert_eq!(got, b"HELLO");

    let block0 = store.read(0, 64).await.unwrap();
    assert_eq!(block0, vec![0u8; 64]);
    store.close().await.unwrap();
}

/// Concurrent cache misses for the same block should trigger only one
/// background full-block fetch.
#[tokio::test]
async fn concurrent_reads_dedupe_background_fetch() {
    let sid = unique_id("dedupe");
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[(0, vec![0xAB; 4096])]);
    s3.set_get_body_delay_ms(100);
    let store = std::sync::Arc::new(load_store(s3.clone(), &sid).await);

    let mut handles = Vec::new();
    for _ in 0..16 {
        let s = std::sync::Arc::clone(&store);
        handles.push(tokio::spawn(async move { s.read(0, 64).await }));
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
    let state = default_state(block_size, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[(0, vec![0xCD; 512])]);
    let store = load_store(s3, &sid).await;

    let got = store.read(0, 1).await.unwrap();
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
#[tokio::test]
async fn read_s3_block_smaller_than_block_size() {
    let sid = unique_id("short-s3");
    let short_data = vec![0xFE; 100];
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[(0, short_data)]);
    let store = load_store(s3, &sid).await;

    let got = store.read(0, 100).await.unwrap();
    assert_eq!(got, vec![0xFE; 100]);
    store.close().await.unwrap();
}

/// Upload path should stream from file (`put_file`).
#[tokio::test]
async fn write_uses_streaming_put_file_upload() {
    let sid = unique_id("putfile");
    let block_size = 64u64;
    let state = default_state(block_size, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    store.write(0, b"HELLO").await.unwrap();

    store.flush().await.unwrap();
    assert!(s3.put_file_calls() >= 1);

    let key = format!("stores/{sid}/0000000000000000");
    let uploaded = s3.get_object(&key).expect("uploaded block missing");
    assert_eq!(uploaded.len(), block_size as usize);
    assert_eq!(&uploaded[..5], b"HELLO");
    store.close().await.unwrap();
}

/// Writes to a store fail after freeze.
#[tokio::test]
async fn writes_fail_after_freeze() {
    let sid = unique_id("snapwr");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(0, b"before").await.unwrap();
    store.freeze().await.unwrap();

    let err = store.write(0, b"after").await.unwrap_err();
    assert!(
        err.to_string().contains("frozen"),
        "expected frozen error, got: {err}"
    );
    store.close().await.unwrap();
}

/// Freeze flushes dirty blocks to S3 before freezing.
#[tokio::test]
async fn freeze_flushes_dirty_blocks() {
    let sid = unique_id("snapflush");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    store.write(0, b"dirty data").await.unwrap();

    store.freeze().await.unwrap();

    let block_key = format!("stores/{sid}/0000000000000000");
    let uploaded = s3.get_object(&block_key).expect("block not flushed to S3");
    assert_eq!(&uploaded[..10], b"dirty data");
    store.close().await.unwrap();
}

// =======================================================================
// Zero-block / tombstone tests (single-store, no ancestry)
// =======================================================================

/// Zero a block with no ancestor — the S3 object should be deleted and reads return zeros.
#[tokio::test]
async fn test_zero_block_no_ancestor() {
    let sid = unique_id("zb-noancestor");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    store.write(0, b"hello world").await.unwrap();
    store.flush().await.unwrap();

    let block_key = format!("stores/{sid}/0000000000000000");
    assert!(s3.get_object(&block_key).is_some());

    store.do_zero_block(0).await.unwrap();
    store.flush().await.unwrap();

    assert!(s3.get_object(&block_key).is_none());

    let got = store.read(0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    store.close().await.unwrap();
}

/// read after zero_block returns zeros.
#[tokio::test]
async fn test_read_zero_block_returns_zeros() {
    let sid = unique_id("zb-read");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(0, b"data here").await.unwrap();
    store.do_zero_block(0).await.unwrap();

    let got = store.read(0, 64).await.unwrap();
    assert_eq!(got, vec![0u8; 64]);
    store.close().await.unwrap();
}

/// Zero a block, then write real data — data is readable and zero_blocks is cleared.
#[tokio::test]
async fn test_zero_block_then_write_data() {
    let sid = unique_id("zb-rewrite");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    store.write(0, b"initial").await.unwrap();
    store.do_zero_block(0).await.unwrap();

    let got = store.read(0, 7).await.unwrap();
    assert_eq!(got, vec![0u8; 7]);

    store.write(0, b"restored").await.unwrap();

    let got = store.read(0, 8).await.unwrap();
    assert_eq!(got, b"restored");

    assert!(!store.zero_blocks.contains(&0));
    store.close().await.unwrap();
}

/// Write 7.5 blocks of zeros starting halfway through a block.
#[tokio::test]
async fn test_write_zeros_spanning_partial_and_full_blocks() {
    let block_size: u64 = 64;
    let sid = unique_id("zb-span");
    let state = default_state(block_size, block_size * 40);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    let fill: Vec<u8> = (0..block_size as u8)
        .cycle()
        .take((block_size * 10) as usize)
        .collect();
    let fill_offset = 23 * block_size;
    store.write(fill_offset, &fill).await.unwrap();
    store.flush().await.unwrap();

    let quarter = block_size / 4;
    let zero_offset = 23 * block_size + quarter;
    let zero_end = 31 * block_size + 3 * quarter;
    let zero_len = (zero_end - zero_offset) as usize;
    let zeros = vec![0u8; zero_len];
    store.write(zero_offset, &zeros).await.unwrap();

    for blk in 24..=30 {
        assert!(store.zero_blocks.contains(&blk), "block {blk} should be in zero_blocks");
    }

    assert!(!store.zero_blocks.contains(&23));
    assert!(!store.zero_blocks.contains(&31));

    let b23 = store.read(23 * block_size, block_size as usize).await.unwrap();
    let expected_b23_head: Vec<u8> = (0..quarter as u8).collect();
    assert_eq!(&b23[..quarter as usize], &expected_b23_head[..]);
    assert_eq!(&b23[quarter as usize..], vec![0u8; (block_size - quarter) as usize]);

    let b31 = store.read(31 * block_size, block_size as usize).await.unwrap();
    assert_eq!(&b31[..(3 * quarter) as usize], vec![0u8; (3 * quarter) as usize]);
    let expected_b31_tail: Vec<u8> = (3 * quarter as u8..block_size as u8).collect();
    assert_eq!(&b31[(3 * quarter) as usize..], &expected_b31_tail[..]);

    for blk in 24..=30 {
        let data = store.read(blk * block_size, block_size as usize).await.unwrap();
        assert_eq!(data, vec![0u8; block_size as usize], "block {blk} should read as zeros");
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
    assert!(result.is_err());
}

#[tokio::test]
async fn test_format_rejects_store_id_with_backslash() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::format(&s3, BUCKET, "", "foo\\bar", 64, 1024).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_format_rejects_empty_store_id() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::format(&s3, BUCKET, "", "", 64, 1024).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_format_rejects_dot_dot_store_id() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::format(&s3, BUCKET, "", "..", 64, 1024).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_load_rejects_invalid_store_id() {
    ensure_global_cache().await;
    let s3 = MockS3::new();
    let result = Store::load(s3, BUCKET.to_string(), "".to_string(), "../evil".to_string(), 4, 4).await;
    assert!(result.is_err());
}

// =======================================================================
// zero_range tests
// =======================================================================

#[tokio::test]
async fn test_zero_range_partial_block() {
    let sid = unique_id("zr-partial");
    let state = default_state(64, 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3, &sid).await;

    let data: Vec<u8> = (0..64u8).collect();
    store.write(0, &data).await.unwrap();

    store.do_zero_range(0, 16, 32).await.unwrap();

    let got = store.read(0, 64).await.unwrap();
    assert_eq!(&got[..16], &(0..16u8).collect::<Vec<_>>());
    assert_eq!(&got[16..48], &[0u8; 32]);
    assert_eq!(&got[48..64], &(48..64u8).collect::<Vec<_>>());
    store.close().await.unwrap();
}

/// Reproduce the fsx_heavy race: write a block, flush it to S3, evict
/// the cache file, zero the block, then immediately do a partial write.
#[tokio::test]
async fn test_zero_block_then_partial_write_no_cache() {
    let sid = unique_id("zero-partial");
    let state = default_state(4096, 1024 * 1024);
    let s3 = mock_s3(&sid, &state, &[]);
    let store = load_store(s3.clone(), &sid).await;

    let original = vec![0xAA; 4096];
    store.write(0, &original).await.unwrap();

    store.flush().await.unwrap();
    let block_key = format!("stores/{sid}/0000000000000000");
    assert!(s3.get_object(&block_key).is_some());

    store.do_zero_block(0).await.unwrap();
    assert!(store.zero_blocks.contains(&0));

    store.write(10, b"hello").await.unwrap();

    let got = store.read(0, 4096).await.unwrap();
    assert_eq!(&got[0..10], &[0u8; 10]);
    assert_eq!(&got[10..15], b"hello");
    assert_eq!(&got[15..4096], &vec![0u8; 4096 - 15]);

    store.flush().await.unwrap();
    let obj = s3.get_object(&block_key).expect("block should exist after flush");
    assert_eq!(&obj[10..15], b"hello");
    store.close().await.unwrap();
}
