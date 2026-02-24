//! S3 volume name → UUID mapping.
//!
//! Each volume name is stored as a separate S3 object at:
//!   `s3://bucket/[prefix/]volumes/<volume-name>`
//!
//! Contents: small JSON `{"uuid": "<store-uuid>"}`.
//!
//! This avoids CAS contention — each volume is an independent object.

use crate::s3::S3Access;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeRef {
    pub uuid: String,
}

/// Build the S3 key prefix for the volumes directory.
/// Mirrors `normalize_prefix` but targets `volumes/` instead of `stores/`.
fn volumes_prefix(prefix: &str) -> String {
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        "volumes".to_string()
    } else {
        format!("{p}/volumes")
    }
}

fn volume_key(prefix: &str, name: &str) -> String {
    let vp = volumes_prefix(prefix);
    format!("{vp}/{name}")
}

/// Read a single volume name mapping from S3.
pub async fn read_volume_ref<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
    name: &str,
) -> Result<VolumeRef> {
    let key = volume_key(prefix, name);
    let bytes = s3
        .get_bytes(bucket, &key)
        .await
        .with_context(|| format!("reading volume ref for {name:?}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing volume ref for {name:?}"))
}

/// Create a new volume name mapping in S3. Uses conditional write to prevent
/// overwriting an existing mapping. Returns an error if the name already exists.
pub async fn create_volume_ref<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
    name: &str,
    uuid: &str,
) -> Result<()> {
    let key = volume_key(prefix, name);
    let vol_ref = VolumeRef {
        uuid: uuid.to_string(),
    };
    let body = serde_json::to_vec(&vol_ref).context("serializing volume ref")?;
    let created = s3
        .put_bytes_if_not_exists(bucket, &key, body)
        .await
        .with_context(|| format!("creating volume ref for {name:?}"))?;
    anyhow::ensure!(created, "volume name {name:?} already exists");
    Ok(())
}

/// Overwrite an existing volume name mapping in S3 (unconditional).
/// Used when updating a mapping after snapshot/clone.
pub async fn update_volume_ref<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
    name: &str,
    uuid: &str,
) -> Result<()> {
    let key = volume_key(prefix, name);
    let vol_ref = VolumeRef {
        uuid: uuid.to_string(),
    };
    let body = serde_json::to_vec(&vol_ref).context("serializing volume ref")?;
    s3.put_bytes(bucket, &key, body)
        .await
        .with_context(|| format!("updating volume ref for {name:?}"))
}

/// List all volume name mappings under the `volumes/` prefix.
pub async fn list_volume_refs<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<(String, VolumeRef)>> {
    let vp = volumes_prefix(prefix);
    let list_prefix = format!("{vp}/");
    let keys = s3
        .list_keys(bucket, &list_prefix)
        .await
        .context("listing volume refs")?;

    let mut result = Vec::new();
    for (key, _size) in keys {
        let name = key.trim_start_matches(&list_prefix).to_string();
        if name.is_empty() || name.contains('/') {
            continue;
        }
        let bytes = s3
            .get_bytes(bucket, &key)
            .await
            .with_context(|| format!("reading volume ref {name:?}"))?;
        match serde_json::from_slice::<VolumeRef>(&bytes) {
            Ok(vol_ref) => result.push((name, vol_ref)),
            Err(e) => {
                tracing::warn!(name = %name, error = %e, "skipping malformed volume ref");
            }
        }
    }
    Ok(result)
}

/// Delete a volume name mapping from S3.
pub async fn delete_volume_ref<S: S3Access>(
    s3: &S,
    bucket: &str,
    prefix: &str,
    name: &str,
) -> Result<()> {
    let key = volume_key(prefix, name);
    s3.delete_object(bucket, &key)
        .await
        .with_context(|| format!("deleting volume ref for {name:?}"))
}
