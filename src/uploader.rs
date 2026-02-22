use crate::cache;
use crate::s3::{S3Access, block_key, upload_block};
use crate::store::Store;
use dashmap::{DashMap, DashSet};
use metrics::{counter, gauge};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};
use tokio::task::JoinSet;
use tracing::{debug, warn};

/// Maximum number of upload retry attempts before giving up on a block.
const MAX_UPLOAD_RETRIES: u32 = 5;

pub fn spawn<S: S3Access + 'static>(
    store: Weak<Store<S>>,
    mut rx: mpsc::Receiver<u64>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let retry_counts: Arc<DashMap<u64, u32>> = Arc::new(DashMap::new());
        let inflight: Arc<DashSet<u64>> = Arc::new(DashSet::new());
        let mut workers: JoinSet<()> = JoinSet::new();
        let mut shutting_down = false;

        loop {
            tokio::select! {
                changed = shutdown_rx.changed(), if !shutting_down => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        shutting_down = true;
                        rx.close();
                    }
                }
                maybe_idx = rx.recv(), if !shutting_down => {
                    match maybe_idx {
                        Some(block_idx) => {
                            let Some(store) = store.upgrade() else {
                                shutting_down = true;
                                rx.close();
                                continue;
                            };

                            // If the block was zeroed, handle the S3 tombstone/delete here
                            // instead of doing a normal upload. Zero-block ops are cheap
                            // (single small PUT or DELETE), so run them inline without
                            // consuming an upload slot.
                            if store.zero_blocks.contains(&block_idx) {
                                handle_zero_block(&store, block_idx, &retry_counts).await;
                                continue;
                            }

                            // Skip if this block already has an upload task in flight.
                            if !inflight.insert(block_idx) {
                                continue;
                            }

                            // Acquire upload slot BEFORE copying so the number of .uploading
                            // files on disk is bounded by max_uploads.
                            let permit = store
                                .upload_slots
                                .clone()
                                .acquire_owned()
                                .await
                                .expect("semaphore closed");

                            let store = Arc::clone(&store);
                            let retry_counts = Arc::clone(&retry_counts);
                            let inflight = Arc::clone(&inflight);

                            workers.spawn(async move {
                                let requeue_delay =
                                    upload_one_block(&store, block_idx, permit, &retry_counts).await;
                                inflight.remove(&block_idx);
                                if let Some(delay) = requeue_delay {
                                    if !delay.is_zero() {
                                        tokio::time::sleep(delay).await;
                                    }
                                    let _ = store.upload_tx.send(block_idx).await;
                                }
                            });
                        }
                        None => {
                            shutting_down = true;
                        }
                    }
                }
                joined = workers.join_next(), if !workers.is_empty() => {
                    if let Some(Err(e)) = joined {
                        warn!(error = %e, "uploader worker task join error");
                    }
                }
                else => {
                    break;
                }
            }

            if shutting_down && workers.is_empty() {
                break;
            }
        }
    })
}

async fn handle_zero_block<S: S3Access>(
    store: &Store<S>,
    block_idx: u64,
    retry_counts: &DashMap<u64, u32>,
) {
    counter!("upload.zero_block.total").increment(1);
    let key = block_key(&store.prefix, &store.id, block_idx);
    let ancestor_has_block = store
        .ancestors
        .iter()
        .any(|(_, idx)| idx.contains(&block_idx));
    let result = if ancestor_has_block {
        store.s3.put_bytes(&store.bucket, &key, Vec::new()).await
    } else if store.local_index.contains(&block_idx) {
        store.s3.delete_object(&store.bucket, &key).await
    } else {
        Ok(())
    };
    match result {
        Ok(()) => {
            if ancestor_has_block {
                store.local_index.insert(block_idx);
            } else {
                store.local_index.remove(&block_idx);
            }
            let _ = cache::get().clear_uploading(&store.id, block_idx).await;
            let _ = cache::get().clear_dirty(&store.id, block_idx).await;
            // If state changed while the zero op was in flight, this zero result
            // is stale. Keep the block pending and run another upload cycle.
            let still_zero = store.zero_blocks.contains(&block_idx);
            if still_zero {
                store.pending_uploads.remove(&block_idx);
                let _ = cache::get().complete_zero_op(&store.id, block_idx).await;
                retry_counts.remove(&block_idx);
                store.upload_epoch.send_modify(|v| *v += 1);
            } else {
                let _ = cache::get().clear_zero_op(&store.id, block_idx).await;
                store.pending_uploads.insert(block_idx);
                retry_counts.remove(&block_idx);
                let _ = store.upload_tx.send(block_idx).await;
            }
        }
        Err(e) => {
            warn!(block = block_idx, error = %e, "zero-block S3 op failed, will retry");
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = store.upload_tx.send(block_idx).await;
        }
    }
}

async fn upload_one_block<S: S3Access>(
    store: &Store<S>,
    block_idx: u64,
    _permit: OwnedSemaphorePermit,
    retry_counts: &DashMap<u64, u32>,
) -> Option<Duration> {
    let _timing = crate::metrics::timing!("upload");
    let lock = store.block_locks.lock(block_idx);

    // Copy to .uploading while holding the block lock so eviction
    // cannot remove the data file during the copy.
    let uploading_path = {
        let _guard = lock.lock().await;
        match cache::get()
            .prepare_upload(&store.id, block_idx, store.state.block_size)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                warn!(block = block_idx, error = %e, "prepare_upload failed");
                let _ = cache::get().mark_dirty(&store.id, block_idx).await;
                store.pending_uploads.insert(block_idx);
                return Some(Duration::from_secs(1));
            }
        }
    };
    // Block lock released — pwrite can now proceed on this block.

    let key = block_key(&store.prefix, &store.id, block_idx);
    debug!(block = block_idx, %key, "uploading block to S3");

    let upload_bytes = std::fs::metadata(&uploading_path)
        .map(|m| m.len())
        .unwrap_or(0);
    match upload_block(&store.s3, &store.bucket, &key, &uploading_path).await {
        Ok(()) => {
            counter!("upload.success").increment(1);
            counter!("upload.bytes").increment(upload_bytes);
            let _ = cache::get().clear_uploading(&store.id, block_idx).await;
            let _ = tokio::fs::remove_file(&uploading_path).await;
            store.local_index.insert(block_idx);
            retry_counts.remove(&block_idx);

            let dirty = cache::get()
                .is_dirty(&store.id, block_idx)
                .await
                .unwrap_or(true);
            if store.zero_blocks.contains(&block_idx) || dirty {
                // Newer data (or a newer zero) landed while this upload was in
                // flight; keep pending and enqueue another pass.
                store.pending_uploads.insert(block_idx);
                debug!(
                    block = block_idx,
                    "upload complete but block changed; requeued"
                );
                Some(Duration::ZERO)
            } else {
                store.pending_uploads.remove(&block_idx);
                gauge!("store.pending_uploads").set(store.pending_uploads.len() as f64);
                store.upload_epoch.send_modify(|v| *v += 1);
                debug!(block = block_idx, "upload complete");
                None
            }
        }
        Err(e) => {
            counter!("upload.failure").increment(1);
            let _ = cache::get().clear_uploading(&store.id, block_idx).await;
            let _ = tokio::fs::remove_file(&uploading_path).await;
            store.pending_uploads.insert(block_idx);
            let _ = cache::get().mark_dirty(&store.id, block_idx).await;
            let count = {
                let mut entry = retry_counts.entry(block_idx).or_insert(0);
                *entry += 1;
                *entry
            };
            if count >= MAX_UPLOAD_RETRIES {
                tracing::error!(
                    block = block_idx,
                    retries = count,
                    error = %e,
                    "upload exhausted retries, resetting retry counter"
                );
                counter!("upload.exhausted_retries").increment(1);
                // Reset counter so retries start fresh, but do NOT
                // remove from pending_uploads — flush() must keep
                // waiting until the block actually lands in S3.
                retry_counts.remove(&block_idx);
                Some(Duration::from_secs(1))
            } else {
                counter!("upload.retry").increment(1);
                let delay = Duration::from_secs(1 << (count - 1).min(5));
                warn!(
                    block = block_idx,
                    retry = count,
                    delay_secs = delay.as_secs(),
                    error = %e,
                    "upload failed, will retry"
                );
                Some(delay)
            }
        }
    }
}
