use crate::store::{BlockStorage, S3Access, Store};
use anyhow::Context;
use ext4_lwext4::{BlockDevice, Error as Ext4Error, Result as Ext4Result};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_EXT4_BLOCK_SIZE: u32 = 4096;
static READ_CALLS: AtomicU64 = AtomicU64::new(0);
static WRITE_CALLS: AtomicU64 = AtomicU64::new(0);
static FLUSH_CALLS: AtomicU64 = AtomicU64::new(0);
static VERBOSE_IO_LOGS: OnceLock<bool> = OnceLock::new();

pub struct StoreBlockDevice<S: S3Access = aws_sdk_s3::Client> {
    store: Arc<Store<S>>,
    rt: tokio::runtime::Handle,
    store_block_size: u64,
    ext4_block_size: u32,
    ext4_block_count: u64,
}

impl<S: S3Access> StoreBlockDevice<S> {
    fn verbose_io_logs_enabled() -> bool {
        *VERBOSE_IO_LOGS.get_or_init(|| {
            std::env::var("LOOPHOLE_LWEXT4_DEBUG")
                .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false)
        })
    }

    pub fn new(store: Arc<Store<S>>, rt: tokio::runtime::Handle) -> anyhow::Result<Self> {
        Self::with_ext4_block_size(store, rt, DEFAULT_EXT4_BLOCK_SIZE)
    }

    pub fn with_ext4_block_size(
        store: Arc<Store<S>>,
        rt: tokio::runtime::Handle,
        ext4_block_size: u32,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(ext4_block_size > 0, "ext4 block size must be > 0");
        anyhow::ensure!(
            store
                .state
                .volume_size
                .is_multiple_of(ext4_block_size as u64),
            "volume size {} must be multiple of ext4 block size {}",
            store.state.volume_size,
            ext4_block_size
        );
        anyhow::ensure!(
            store.state.block_size > 0,
            "store block size must be > 0 (got {})",
            store.state.block_size
        );

        Ok(Self {
            store_block_size: store.state.block_size,
            ext4_block_count: store.state.volume_size / ext4_block_size as u64,
            store,
            rt,
            ext4_block_size,
        })
    }

    fn to_io_error(err: anyhow::Error) -> Ext4Error {
        Ext4Error::Io(std::io::Error::other(format!("{err:#}")))
    }

    fn checked_block_offset(&self, block_id: u64) -> Ext4Result<u64> {
        block_id
            .checked_mul(self.ext4_block_size as u64)
            .ok_or_else(|| Ext4Error::InvalidArgument("block offset overflow".to_string()))
    }

    fn checked_end(&self, start: u64, len: usize) -> Ext4Result<u64> {
        start
            .checked_add(len as u64)
            .ok_or_else(|| Ext4Error::InvalidArgument("range end overflow".to_string()))
    }

    fn run_blocking<F>(&self, fut: F) -> F::Output
    where
        F: std::future::Future,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.rt.block_on(fut))
        } else {
            self.rt.block_on(fut)
        }
    }

    fn maybe_log_io(kind: &str, calls: &AtomicU64, block_id: u64, bytes: usize) {
        if cfg!(test) && Self::verbose_io_logs_enabled() {
            let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
            if n <= 20 || n.is_multiple_of(200) {
                eprintln!("[blockdev:{kind}] call={n} block_id={block_id} bytes={bytes}");
            }
        }
    }
}

impl<S: S3Access> BlockDevice for StoreBlockDevice<S> {
    fn read_blocks(&self, block_id: u64, buf: &mut [u8]) -> Ext4Result<u32> {
        Self::maybe_log_io("read", &READ_CALLS, block_id, buf.len());
        if buf.is_empty() {
            return Ok(0);
        }
        if !buf.len().is_multiple_of(self.ext4_block_size as usize) {
            return Err(Ext4Error::InvalidArgument(format!(
                "read buffer length {} not aligned to ext4 block size {}",
                buf.len(),
                self.ext4_block_size
            )));
        }

        let start = self.checked_block_offset(block_id)?;
        let end = self.checked_end(start, buf.len())?;
        let device_size = self.block_count() * self.block_size() as u64;
        if end > device_size {
            return Err(Ext4Error::InvalidArgument(format!(
                "read range [{start},{end}) exceeds device size {device_size}"
            )));
        }

        let mut cursor = 0usize;
        while cursor < buf.len() {
            let absolute = start + cursor as u64;
            let store_block_idx = absolute / self.store_block_size;
            let offset_within = absolute % self.store_block_size;
            let chunk =
                ((buf.len() - cursor) as u64).min(self.store_block_size - offset_within) as usize;

            let data = self
                .run_blocking(self.store.read_block(store_block_idx, offset_within, chunk))
                .with_context(|| {
                    format!(
                        "read_block failed: store_block_idx={store_block_idx}, offset={offset_within}, len={chunk}"
                    )
                })
                .map_err(Self::to_io_error)?;

            if data.len() != chunk {
                return Err(Ext4Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("short read: got {}, expected {}", data.len(), chunk),
                )));
            }

            buf[cursor..cursor + chunk].copy_from_slice(&data);
            cursor += chunk;
        }

        Ok((buf.len() / self.ext4_block_size as usize) as u32)
    }

    fn write_blocks(&mut self, block_id: u64, buf: &[u8]) -> Ext4Result<u32> {
        Self::maybe_log_io("write", &WRITE_CALLS, block_id, buf.len());
        if buf.is_empty() {
            return Ok(0);
        }
        if !buf.len().is_multiple_of(self.ext4_block_size as usize) {
            return Err(Ext4Error::InvalidArgument(format!(
                "write buffer length {} not aligned to ext4 block size {}",
                buf.len(),
                self.ext4_block_size
            )));
        }

        let start = self.checked_block_offset(block_id)?;
        let end = self.checked_end(start, buf.len())?;
        let device_size = self.block_count() * self.block_size() as u64;
        if end > device_size {
            return Err(Ext4Error::InvalidArgument(format!(
                "write range [{start},{end}) exceeds device size {device_size}"
            )));
        }

        let mut cursor = 0usize;
        while cursor < buf.len() {
            let absolute = start + cursor as u64;
            let store_block_idx = absolute / self.store_block_size;
            let offset_within = absolute % self.store_block_size;
            let chunk =
                ((buf.len() - cursor) as u64).min(self.store_block_size - offset_within) as usize;

            self.run_blocking(
                self.store
                    .write_block(store_block_idx, offset_within, &buf[cursor..cursor + chunk]),
            )
                .with_context(|| {
                    format!(
                        "write_block failed: store_block_idx={store_block_idx}, offset={offset_within}, len={chunk}"
                    )
                })
                .map_err(Self::to_io_error)?;

            cursor += chunk;
        }

        Ok((buf.len() / self.ext4_block_size as usize) as u32)
    }

    fn flush(&mut self) -> Ext4Result<()> {
        Self::maybe_log_io("flush", &FLUSH_CALLS, 0, 0);
        self.run_blocking(self.store.flush())
            .context("store flush failed")
            .map_err(Self::to_io_error)
    }

    fn block_size(&self) -> u32 {
        self.ext4_block_size
    }

    fn block_count(&self) -> u64 {
        self.ext4_block_count
    }
}
