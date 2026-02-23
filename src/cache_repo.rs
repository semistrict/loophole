use anyhow::{Context, Result};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroOpKind {
    Tombstone = 0,
    Delete = 1,
}

impl ZeroOpKind {
    pub fn from_i64(v: i64) -> Result<Self> {
        match v {
            0 => Ok(Self::Tombstone),
            1 => Ok(Self::Delete),
            _ => anyhow::bail!("invalid zero op kind: {v}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingZeroOp {
    pub block_idx: u64,
    pub kind: ZeroOpKind,
}

#[derive(Debug, Default)]
pub struct RecoveryWork {
    pub dirty_blocks: Vec<u64>,
    pub pending_zero_ops: Vec<PendingZeroOp>,
}

#[derive(Debug, Clone, Copy)]
pub struct EvictionBlockRef {
    pub block_id: i64,
    pub dirty: bool,
    pub uploading: bool,
}

pub struct CacheRepo {
    pool: SqlitePool,
}

impl CacheRepo {
    pub async fn open(db_path: &Path) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(16)
            .connect_with(opts)
            .await
            .context("opening sqlite cache")?;
        let repo = Self { pool };
        repo.create_schema().await?;
        Ok(repo)
    }

    pub async fn init_store(&self, store_id: &str, parent_store_id: Option<&str>) -> Result<i64> {
        sqlx::query!(
            "INSERT OR IGNORE INTO volumes(volume_name) VALUES (?)",
            store_id
        )
        .execute(&self.pool)
        .await
        .with_context(|| format!("creating volume row for {store_id}"))?;

        if let Some(parent) = parent_store_id {
            sqlx::query!(
                "INSERT OR IGNORE INTO volumes(volume_name) VALUES (?)",
                parent
            )
            .execute(&self.pool)
            .await
            .with_context(|| format!("creating parent volume row for {parent}"))?;

            sqlx::query!(
                "UPDATE volumes
                 SET parent_volume_id=(SELECT volume_id FROM volumes WHERE volume_name=?)
                 WHERE volume_name=?",
                parent,
                store_id
            )
            .execute(&self.pool)
            .await
            .context("updating parent volume id")?;
        }

        self.lookup_volume_id(store_id).await
    }

    pub async fn lookup_volume_id(&self, store_id: &str) -> Result<i64> {
        let row = sqlx::query!(
            r#"SELECT volume_id as "volume_id!: i64" FROM volumes WHERE volume_name=?"#,
            store_id
        )
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or_else(|| anyhow::anyhow!("volume not initialized: {store_id}"))?;
        Ok(row.volume_id)
    }

    pub async fn list_blocks_for_lru(&self, volume_id: i64) -> Result<Vec<u64>> {
        let rows = sqlx::query!(
            r#"SELECT block_idx as "block_idx!: i64"
               FROM blocks WHERE volume_id=? ORDER BY block_idx"#,
            volume_id
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.block_idx as u64).collect())
    }

    pub async fn lookup_block(&self, volume_id: i64, block_idx: u64) -> Result<Option<i64>> {
        let block_idx_i64 = block_idx as i64;
        let row = sqlx::query!(
            r#"SELECT block_id as "block_id!: i64"
               FROM blocks
               WHERE volume_id=? AND block_idx=?"#,
            volume_id,
            block_idx_i64
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.block_id))
    }

    pub async fn has_block(&self, volume_id: i64, block_idx: u64) -> Result<bool> {
        Ok(self.lookup_block(volume_id, block_idx).await?.is_some())
    }

    pub async fn is_populated(&self, volume_id: i64, block_idx: u64) -> Result<bool> {
        let block_idx_i64 = block_idx as i64;
        let row = sqlx::query!(
            r#"SELECT populated as "populated!: bool"
               FROM blocks
               WHERE volume_id=? AND block_idx=?"#,
            volume_id,
            block_idx_i64
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.populated).unwrap_or(false))
    }

    pub async fn ensure_block_row(&self, volume_id: i64, block_idx: u64) -> Result<i64> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "INSERT OR IGNORE INTO blocks(volume_id, block_idx, populated, dirty_at, uploading_at)
             VALUES(?, ?, FALSE, NULL, NULL)",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?;

        let row = sqlx::query!(
            r#"SELECT block_id as "block_id!: i64" FROM blocks WHERE volume_id=? AND block_idx=?"#,
            volume_id,
            block_idx_i64
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row.block_id)
    }

    pub async fn mark_populated(&self, volume_id: i64, block_idx: u64) -> Result<()> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "UPDATE blocks SET populated=TRUE WHERE volume_id=? AND block_idx=?",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn try_mark_dirty(&self, volume_id: i64, block_idx: u64) -> Result<bool> {
        let block_idx_i64 = block_idx as i64;
        let changed = sqlx::query!(
            "UPDATE blocks
             SET dirty_at=unixepoch()
             WHERE volume_id=? AND block_idx=? AND dirty_at IS NULL",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed == 1)
    }

    pub async fn mark_dirty(&self, volume_id: i64, block_idx: u64) -> Result<()> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "UPDATE blocks SET dirty_at=unixepoch() WHERE volume_id=? AND block_idx=?",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn clear_dirty(&self, volume_id: i64, block_idx: u64) -> Result<()> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "UPDATE blocks SET dirty_at=NULL WHERE volume_id=? AND block_idx=?",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn clear_uploading(&self, volume_id: i64, block_idx: u64) -> Result<()> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "UPDATE blocks SET uploading_at=NULL WHERE volume_id=? AND block_idx=?",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn is_dirty(&self, volume_id: i64, block_idx: u64) -> Result<bool> {
        let block_idx_i64 = block_idx as i64;
        let row = sqlx::query!(
            r#"SELECT dirty_at as "dirty_at: i64" FROM blocks WHERE volume_id=? AND block_idx=?"#,
            volume_id,
            block_idx_i64
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| r.dirty_at).is_some())
    }

    pub async fn begin_upload(&self, volume_id: i64, block_idx: u64) -> Result<()> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "UPDATE blocks SET dirty_at=NULL, uploading_at=unixepoch() WHERE volume_id=? AND block_idx=?",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_dirty_blocks(&self, volume_id: i64) -> Result<Vec<u64>> {
        let rows = sqlx::query!(
            r#"SELECT block_idx as "block_idx!: i64"
               FROM blocks WHERE volume_id=? AND dirty_at IS NOT NULL
               ORDER BY block_idx"#,
            volume_id
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.block_idx as u64).collect())
    }

    pub async fn remove_block(&self, volume_id: i64, block_idx: u64) -> Result<bool> {
        let block_idx_i64 = block_idx as i64;
        let rows = sqlx::query!(
            "DELETE FROM blocks WHERE volume_id=? AND block_idx=?",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(rows > 0)
    }

    pub async fn queue_zero_op(
        &self,
        volume_id: i64,
        block_idx: u64,
        kind: ZeroOpKind,
    ) -> Result<()> {
        let block_idx_i64 = block_idx as i64;
        let op = kind as i64;
        sqlx::query!(
            "INSERT INTO pending_zero_ops(volume_id, block_idx, op, enqueued_at, attempts)
             VALUES(?, ?, ?, unixepoch(), 0)
             ON CONFLICT(volume_id, block_idx) DO UPDATE SET
                op=excluded.op,
                enqueued_at=excluded.enqueued_at",
            volume_id,
            block_idx_i64,
            op
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn clear_zero_op(&self, volume_id: i64, block_idx: u64) -> Result<()> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "DELETE FROM pending_zero_ops WHERE volume_id=? AND block_idx=?",
            volume_id,
            block_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn recover(&self, volume_id: i64) -> Result<RecoveryWork> {
        // Delete block rows that were mid-download (not yet populated).
        // The associated files are cleaned up by the cache layer.
        sqlx::query!(
            "DELETE FROM blocks WHERE volume_id=? AND populated=FALSE AND dirty_at IS NULL AND uploading_at IS NULL",
            volume_id
        )
        .execute(&self.pool)
        .await?;

        // Move uploading → dirty for retry.
        sqlx::query!(
            "UPDATE blocks
             SET dirty_at=COALESCE(dirty_at, unixepoch()), uploading_at=NULL
             WHERE volume_id=? AND uploading_at IS NOT NULL",
            volume_id
        )
        .execute(&self.pool)
        .await?;

        let dirty_rows = sqlx::query!(
            r#"SELECT block_idx as "block_idx!: i64"
                FROM blocks WHERE volume_id=? AND dirty_at IS NOT NULL"#,
            volume_id
        )
        .fetch_all(&self.pool)
        .await?;
        let dirty_blocks = dirty_rows.into_iter().map(|r| r.block_idx as u64).collect();

        let zero_rows = sqlx::query!(
            r#"SELECT block_idx as "block_idx!: i64", op as "op!: i64"
               FROM pending_zero_ops WHERE volume_id=?"#,
            volume_id
        )
        .fetch_all(&self.pool)
        .await?;
        let mut pending_zero_ops = Vec::with_capacity(zero_rows.len());
        for r in zero_rows {
            pending_zero_ops.push(PendingZeroOp {
                block_idx: r.block_idx as u64,
                kind: ZeroOpKind::from_i64(r.op)?,
            });
        }

        Ok(RecoveryWork {
            dirty_blocks,
            pending_zero_ops,
        })
    }

    pub async fn lookup_block_for_eviction(
        &self,
        volume_id: i64,
        block_idx: u64,
    ) -> Result<Option<EvictionBlockRef>> {
        let block_idx_i64 = block_idx as i64;
        let row = sqlx::query!(
            r#"SELECT block_id as "block_id!: i64",
                      dirty_at as "dirty_at: i64",
                      uploading_at as "uploading_at: i64"
               FROM blocks
               WHERE volume_id=? AND block_idx=?"#,
            volume_id,
            block_idx_i64
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| EvictionBlockRef {
            block_id: r.block_id,
            dirty: r.dirty_at.is_some(),
            uploading: r.uploading_at.is_some(),
        }))
    }

    pub async fn delete_block_by_id(&self, block_id: i64) -> Result<()> {
        sqlx::query!("DELETE FROM blocks WHERE block_id=?", block_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn create_schema(&self) -> Result<()> {
        // Drop legacy chunks table from older schema versions.
        sqlx::query!("DROP TABLE IF EXISTS chunks")
            .execute(&self.pool)
            .await?;

        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS volumes (
                volume_id        INTEGER PRIMARY KEY,
                volume_name      TEXT    NOT NULL UNIQUE,
                parent_volume_id INTEGER REFERENCES volumes(volume_id)
            )"
        )
        .execute(&self.pool)
        .await?;

        // Check if blocks table exists with old schema (has 'size' or 'downloaded_thru' columns).
        let has_old_schema = sqlx::query_scalar::<_, i32>(
            "SELECT COUNT(*) FROM pragma_table_info('blocks') WHERE name='downloaded_thru'",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);
        if has_old_schema > 0 {
            sqlx::query!("DROP TABLE blocks")
                .execute(&self.pool)
                .await?;
        }

        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS blocks (
                block_id        INTEGER PRIMARY KEY,
                volume_id       INTEGER NOT NULL REFERENCES volumes(volume_id),
                block_idx       INTEGER NOT NULL,
                populated       BOOLEAN NOT NULL DEFAULT FALSE,
                dirty_at        INTEGER,
                uploading_at    INTEGER,
                UNIQUE (volume_id, block_idx)
            )"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS pending_zero_ops (
                volume_id   INTEGER NOT NULL REFERENCES volumes(volume_id),
                block_idx   INTEGER NOT NULL,
                op          INTEGER NOT NULL,
                enqueued_at INTEGER NOT NULL,
                attempts    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (volume_id, block_idx)
            )"
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
