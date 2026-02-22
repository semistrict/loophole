use anyhow::{Context, Result};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::path::Path;
use std::time::Duration;

pub const CHUNK_SIZE: u64 = 16 * 1024;

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
pub struct BlockRef {
    pub block_id: i64,
    pub downloaded_thru: Option<i64>,
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

    pub async fn list_blocks_for_lru(&self, volume_id: i64) -> Result<Vec<(u64, u64)>> {
        let rows = sqlx::query!(
            r#"SELECT block_idx as "block_idx!: i64", size as "size!: i64"
               FROM blocks WHERE volume_id=? ORDER BY block_idx"#,
            volume_id
        )
        .fetch_all(&self.pool)
        .await?;
        let out = rows
            .into_iter()
            .map(|r| (r.block_idx as u64, r.size.max(0) as u64))
            .collect();
        Ok(out)
    }

    pub async fn lookup_block(&self, volume_id: i64, block_idx: u64) -> Result<Option<BlockRef>> {
        let block_idx_i64 = block_idx as i64;
        let row = sqlx::query!(
            r#"SELECT block_id as "block_id!: i64", downloaded_thru as "downloaded_thru: i64"
               FROM blocks
               WHERE volume_id=? AND block_idx=?"#,
            volume_id,
            block_idx_i64
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| BlockRef {
            block_id: r.block_id,
            downloaded_thru: r.downloaded_thru,
        }))
    }

    pub async fn has_block(&self, volume_id: i64, block_idx: u64) -> Result<bool> {
        Ok(self.lookup_block(volume_id, block_idx).await?.is_some())
    }

    pub async fn read_chunks_in_range(
        &self,
        block_id: i64,
        first_chunk: u64,
        last_chunk: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let first_chunk_i64 = first_chunk as i64;
        let last_chunk_i64 = last_chunk as i64;
        let rows = sqlx::query!(
            r#"SELECT chunk_idx as "chunk_idx!: i64", data as "data!: Vec<u8>"
               FROM chunks
               WHERE block_id=? AND chunk_idx BETWEEN ? AND ?
               ORDER BY chunk_idx"#,
            block_id,
            first_chunk_i64,
            last_chunk_i64
        )
        .fetch_all(&self.pool)
        .await
        .context("querying chunks for read")?;
        let out = rows
            .into_iter()
            .map(|r| (r.chunk_idx as u64, r.data))
            .collect();
        Ok(out)
    }

    pub async fn ensure_block_row(&self, volume_id: i64, block_idx: u64) -> Result<i64> {
        let block_idx_i64 = block_idx as i64;
        sqlx::query!(
            "INSERT OR IGNORE INTO blocks(volume_id, block_idx, size, downloaded_thru, dirty_at, uploading_at)
             VALUES(?, ?, 0, NULL, NULL, NULL)",
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

    pub async fn load_chunk(&self, block_id: i64, chunk_idx: u64) -> Result<Option<Vec<u8>>> {
        let chunk_idx_i64 = chunk_idx as i64;
        let row = sqlx::query!(
            r#"SELECT data as "data!: Vec<u8>" FROM chunks WHERE block_id=? AND chunk_idx=?"#,
            block_id,
            chunk_idx_i64
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.data))
    }

    pub async fn upsert_chunk(&self, block_id: i64, chunk_idx: u64, data: &[u8]) -> Result<()> {
        let chunk_idx_i64 = chunk_idx as i64;
        sqlx::query!(
            "INSERT OR REPLACE INTO chunks(block_id, chunk_idx, data) VALUES(?, ?, ?)",
            block_id,
            chunk_idx_i64,
            data
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_chunk(&self, block_id: i64, chunk_idx: u64) -> Result<()> {
        let chunk_idx_i64 = chunk_idx as i64;
        sqlx::query!(
            "DELETE FROM chunks WHERE block_id=? AND chunk_idx=?",
            block_id,
            chunk_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_chunks_after(&self, block_id: i64, chunk_idx: u64) -> Result<()> {
        let chunk_idx_i64 = chunk_idx as i64;
        sqlx::query!(
            "DELETE FROM chunks WHERE block_id=? AND chunk_idx>?",
            block_id,
            chunk_idx_i64
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_all_chunks(&self, block_id: i64) -> Result<()> {
        sqlx::query!("DELETE FROM chunks WHERE block_id=?", block_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn chunk_count(&self, block_id: i64) -> Result<u64> {
        let row = sqlx::query!(
            r#"SELECT COUNT(*) as "count!: i64" FROM chunks WHERE block_id=?"#,
            block_id
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row.count as u64)
    }

    pub async fn set_block_size_and_downloaded(
        &self,
        block_id: i64,
        size: u64,
        downloaded_thru: Option<i64>,
    ) -> Result<()> {
        let size_i64 = size as i64;
        sqlx::query!(
            "UPDATE blocks SET size=?, downloaded_thru=? WHERE block_id=?",
            size_i64,
            downloaded_thru,
            block_id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset_stream_download_state(&self, block_id: i64) -> Result<()> {
        sqlx::query!(
            "UPDATE blocks
             SET downloaded_thru=-1, dirty_at=NULL, uploading_at=NULL, size=0
             WHERE block_id=?",
            block_id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_downloaded_thru(&self, block_id: i64, chunk_idx: u64) -> Result<()> {
        let chunk_idx_i64 = chunk_idx as i64;
        sqlx::query!(
            "UPDATE blocks SET downloaded_thru=? WHERE block_id=?",
            chunk_idx_i64,
            block_id
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

    pub async fn begin_upload(&self, volume_id: i64, block_idx: u64) -> Result<i64> {
        let block = self
            .lookup_block(volume_id, block_idx)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("prepare_upload for missing block {volume_id}/{block_idx}")
            })?;
        sqlx::query!(
            "UPDATE blocks SET dirty_at=NULL, uploading_at=unixepoch() WHERE block_id=?",
            block.block_id
        )
        .execute(&self.pool)
        .await?;
        Ok(block.block_id)
    }

    pub async fn read_all_chunks(&self, block_id: i64) -> Result<Vec<(u64, Vec<u8>)>> {
        let rows = sqlx::query!(
            r#"SELECT chunk_idx as "chunk_idx!: i64", data as "data!: Vec<u8>"
               FROM chunks WHERE block_id=? ORDER BY chunk_idx"#,
            block_id
        )
        .fetch_all(&self.pool)
        .await?;
        let out = rows
            .into_iter()
            .map(|r| (r.chunk_idx as u64, r.data))
            .collect();
        Ok(out)
    }

    pub async fn remove_block(&self, volume_id: i64, block_idx: u64) -> Result<bool> {
        let Some(block) = self.lookup_block(volume_id, block_idx).await? else {
            return Ok(false);
        };
        sqlx::query!("DELETE FROM chunks WHERE block_id=?", block.block_id)
            .execute(&self.pool)
            .await?;
        sqlx::query!("DELETE FROM blocks WHERE block_id=?", block.block_id)
            .execute(&self.pool)
            .await?;
        Ok(true)
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
        sqlx::query!(
            "DELETE FROM chunks
             WHERE block_id IN (
               SELECT block_id
               FROM blocks
               WHERE volume_id=? AND downloaded_thru IS NOT NULL
             )",
            volume_id
        )
        .execute(&self.pool)
        .await?;
        sqlx::query!(
            "DELETE FROM blocks WHERE volume_id=? AND downloaded_thru IS NOT NULL",
            volume_id
        )
        .execute(&self.pool)
        .await?;
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
        sqlx::query!("DELETE FROM chunks WHERE block_id=?", block_id)
            .execute(&self.pool)
            .await?;
        sqlx::query!("DELETE FROM blocks WHERE block_id=?", block_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn create_schema(&self) -> Result<()> {
        sqlx::query("PRAGMA page_size=65536")
            .execute(&self.pool)
            .await
            .context("setting page_size")?;
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&self.pool)
            .await
            .context("setting journal_mode")?;
        sqlx::query("PRAGMA synchronous=NORMAL")
            .execute(&self.pool)
            .await
            .context("setting synchronous")?;
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&self.pool)
            .await
            .context("setting foreign_keys")?;

        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS volumes (
                volume_id        INTEGER PRIMARY KEY,
                volume_name      TEXT    NOT NULL UNIQUE,
                parent_volume_id INTEGER REFERENCES volumes(volume_id)
            )"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS blocks (
                block_id        INTEGER PRIMARY KEY,
                volume_id       INTEGER NOT NULL REFERENCES volumes(volume_id),
                block_idx       INTEGER NOT NULL,
                size            INTEGER NOT NULL DEFAULT 0,
                downloaded_thru INTEGER,
                dirty_at        INTEGER,
                uploading_at    INTEGER,
                UNIQUE (volume_id, block_idx)
            )"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query!(
            "CREATE TABLE IF NOT EXISTS chunks (
                block_id  INTEGER NOT NULL REFERENCES blocks(block_id),
                chunk_idx INTEGER NOT NULL,
                data      BLOB    NOT NULL,
                PRIMARY KEY (block_id, chunk_idx)
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
