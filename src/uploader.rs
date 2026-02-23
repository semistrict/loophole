use crate::cache;
use crate::s3::{S3Access, block_key, upload_block};
use crate::store::Store;
use metrics::{counter, gauge};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use anyhow::Context;

/// Default interval between upload cycles when no explicit flush is requested.
const DEFAULT_UPLOAD_INTERVAL_SECS: u64 = 30;

/// A work item prepared under lock, executed without locks in the S3 phase.
enum UploadWork {
    /// Upload block data from this temp file.
    Data {
        block_idx: u64,
        temp_path: tempfile::TempPath,
    },
    /// Write a zero-byte tombstone to S3 (ancestor has this block).
    Tombstone { block_idx: u64 },
    /// Delete from S3 (no ancestor, but we uploaded it previously).
    Delete { block_idx: u64 },
}

/// Run one upload cycle: atomically drain writes, snapshot all dirty blocks
/// and zero ops, execute S3 work, then handle any pending snapshot/clone request.
/// Returns Ok((generation, completed_count)) on success. The generation is the
/// maximum generation this cycle can claim to satisfy (captured under write lock).
/// Failed blocks remain dirty in SQLite for the next cycle.
async fn upload_cycle<S: S3Access>(store: &Arc<Store<S>>) -> anyhow::Result<(u64, usize)> {
    let _timing = crate::metrics::timing!("upload.cycle");

    // 1. Atomically drain writes + collect dirty blocks + acquire block locks.
    //    This replaces the old manual list_dirty_blocks + lock acquisition.
    let dirty_set = store.acquire_dirty_blocks().await?;
    let cycle_generation = dirty_set.generation;

    if dirty_set.dirty_blocks.is_empty() && dirty_set.zero_block_indices.is_empty() {
        // No dirty work, but still check for snapshot requests.
        drop(dirty_set);
        store.execute_snapshot_request().await?;
        return Ok((cycle_generation, 0));
    }

    info!(
        dirty = dirty_set.dirty_blocks.len(),
        zero_ops = dirty_set.zero_block_indices.len(),
        "upload cycle starting"
    );

    // 2. Under locks: decide what to do for each block, prepare work items.
    //    Release each lock as soon as the decision is made and data is copied.
    let mut work_items: Vec<UploadWork> = Vec::with_capacity(dirty_set.guards.len());
    for (block_idx, _guard) in dirty_set.guards {
        let is_zero = store.zero_blocks.contains(&block_idx);

        if is_zero {
            // Zero op takes priority — even if the block is also dirty,
            // the zero supersedes it.
            let ancestor_has_block = store
                .ancestors
                .iter()
                .any(|(_, idx)| idx.contains(&block_idx));
            if ancestor_has_block {
                work_items.push(UploadWork::Tombstone { block_idx });
            } else if store.local_index.contains(&block_idx) {
                work_items.push(UploadWork::Delete { block_idx });
            }
            // else: no ancestor, not in S3 — nothing to do, will clean up below
        } else {
            // Regular dirty block — copy data to temp file.
            let src = cache::get().block_path(&store.id, block_idx);
            if !src.exists() {
                let _ = cache::get().clear_dirty(&store.id, block_idx).await;
                continue;
            }

            let temp = tempfile::NamedTempFile::new_in(cache::get().cache_dir())
                .context("creating upload temp file")?;
            let temp_path = temp.into_temp_path();
            std::fs::copy(&src, &temp_path)
                .with_context(|| format!("copying block {} for upload", block_idx))?;

            cache::get().begin_upload(&store.id, block_idx).await?;
            work_items.push(UploadWork::Data {
                block_idx,
                temp_path,
            });
        }
        // Lock released here (guard dropped) — writes can resume on this block.
    }

    // 3. Execute all S3 operations concurrently. No locks held.
    let mut join_set = tokio::task::JoinSet::new();
    let semaphore = Arc::clone(&store.upload_slots);

    for item in work_items {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let store = Arc::clone(store);

        join_set.spawn(async move {
            let _permit = permit;
            match item {
                UploadWork::Data {
                    block_idx,
                    temp_path,
                } => {
                    let key = block_key(&store.prefix, &store.id, block_idx);
                    debug!(block = block_idx, %key, "uploading block to S3");

                    let upload_bytes = std::fs::metadata(&temp_path).map(|m| m.len()).unwrap_or(0);
                    let result = upload_block(&store.s3, &store.bucket, &key, &temp_path).await;
                    // temp_path dropped here (or on early return) — auto-deletes the file.

                    match result {
                        Ok(()) => {
                            counter!("upload.success").increment(1);
                            counter!("upload.bytes").increment(upload_bytes);
                            let _ = cache::get().clear_uploading(&store.id, block_idx).await;
                            store.local_index.insert(block_idx);
                            Ok(block_idx)
                        }
                        Err(e) => {
                            counter!("upload.failure").increment(1);
                            let _ = cache::get().clear_uploading(&store.id, block_idx).await;
                            let _ = cache::get().mark_dirty(&store.id, block_idx).await;
                            warn!(block = block_idx, error = %e, "upload failed");
                            Err(e)
                        }
                    }
                }
                UploadWork::Tombstone { block_idx } => {
                    let key = block_key(&store.prefix, &store.id, block_idx);
                    debug!(block = block_idx, %key, "writing zero tombstone to S3");
                    counter!("upload.zero_block.total").increment(1);

                    match store.s3.put_bytes(&store.bucket, &key, Vec::new()).await {
                        Ok(()) => {
                            store.local_index.insert(block_idx);
                            finish_zero_op(&store, block_idx).await;
                            Ok(block_idx)
                        }
                        Err(e) => {
                            warn!(block = block_idx, error = %e, "tombstone put failed");
                            Err(e)
                        }
                    }
                }
                UploadWork::Delete { block_idx } => {
                    let key = block_key(&store.prefix, &store.id, block_idx);
                    debug!(block = block_idx, %key, "deleting zero block from S3");
                    counter!("upload.zero_block.total").increment(1);

                    match store.s3.delete_object(&store.bucket, &key).await {
                        Ok(()) => {
                            store.local_index.remove(&block_idx);
                            finish_zero_op(&store, block_idx).await;
                            Ok(block_idx)
                        }
                        Err(e) => {
                            warn!(block = block_idx, error = %e, "zero block delete failed");
                            Err(e)
                        }
                    }
                }
            }
        });
    }

    // 4. Collect results.
    let mut completed = 0usize;
    let mut failed = 0usize;
    let mut first_error: Option<anyhow::Error> = None;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(_)) => completed += 1,
            Ok(Err(e)) => {
                failed += 1;
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(e) => {
                failed += 1;
                warn!(error = %e, "upload task panicked");
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!("upload task panicked: {e}"));
                }
            }
        }
    }

    gauge!("store.pending_uploads").set(
        cache::get()
            .list_dirty_blocks(&store.id)
            .await
            .map(|v| v.len())
            .unwrap_or(0) as f64,
    );

    if let Some(e) = first_error {
        anyhow::bail!("upload cycle: {completed} succeeded, {failed} failed — first error: {e}");
    }

    // 5. After successful uploads, execute any pending snapshot/clone request.
    store.execute_snapshot_request().await?;

    Ok((cycle_generation, completed))
}

/// Clean up after a successful zero-block S3 operation.
/// Only applies if the block is still in zero_blocks (no concurrent write overwrote it).
async fn finish_zero_op<S: S3Access>(store: &Arc<Store<S>>, block_idx: u64) {
    let still_zero = store.zero_blocks.contains(&block_idx);
    if still_zero {
        let _ = cache::get().clear_dirty(&store.id, block_idx).await;
        let _ = cache::get().clear_uploading(&store.id, block_idx).await;
        let _ = cache::get().complete_zero_op(&store.id, block_idx).await;
    } else {
        // Block was written while zero op was in flight — discard.
        let _ = cache::get().clear_zero_op(&store.id, block_idx).await;
    }
}

/// Spawn the upload loop. Returns a JoinHandle.
///
/// The loop waits for either:
/// - `requested_generation > completed_generation` (flush requested)
/// - interval timer fires and there are dirty blocks
///
/// Runs exactly one cycle per wake-up. No retries — failed blocks stay dirty
/// for the next cycle. On failure, the error is sent via `completed_generation`
/// so flush() callers see it.
pub fn spawn<S: S3Access + 'static>(
    store: Arc<Store<S>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let interval_secs = std::env::var("LOOPHOLE_UPLOAD_INTERVAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_UPLOAD_INTERVAL_SECS);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // Wait for: flush request, interval tick, or shutdown.
            tokio::select! {
                _ = store.flush_notify.notified() => {}
                _ = interval.tick() => {}
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        // Final upload cycle before shutdown — always report result.
                        match upload_cycle(&store).await {
                            Ok((cycle_gen, _)) => {
                                store.completed_generation.send_modify(|v| {
                                    *v = (cycle_gen.max(v.0), None);
                                });
                            }
                            Err(e) => {
                                store.completed_generation.send_modify(|v| {
                                    v.1 = Some(e.to_string());
                                });
                            }
                        }
                        break;
                    }
                }
            }

            // Run exactly one cycle.
            match upload_cycle(&store).await {
                Ok((cycle_gen, _)) => {
                    store.completed_generation.send_modify(|v| {
                        *v = (cycle_gen.max(v.0), None);
                    });
                }
                Err(e) => {
                    // Keep the generation unchanged; store the error so that
                    // any flush() already waiting at this generation sees it.
                    store.completed_generation.send_modify(|v| {
                        v.1 = Some(e.to_string());
                    });
                }
            }
        }
    })
}
