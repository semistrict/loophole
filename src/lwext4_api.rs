use crate::blockdev_adapter::StoreBlockDevice;
use crate::store::{S3Access, Store};
use anyhow::Context;
use ext4_lwext4::{Ext4Fs, MkfsOptions};
use std::sync::{Arc, Mutex};

const EXT4_BLOCK_SIZE: u32 = 4096;

pub struct Lwext4Volume<S: S3Access = aws_sdk_s3::Client> {
    fs: Mutex<Ext4Fs>,
    store: Arc<Store<S>>,
    rt: tokio::runtime::Handle,
}

impl<S: S3Access> Lwext4Volume<S> {
    pub async fn format(store: Arc<Store<S>>) -> anyhow::Result<()> {
        let rt = tokio::runtime::Handle::current();
        let device =
            StoreBlockDevice::with_ext4_block_size(Arc::clone(&store), rt, EXT4_BLOCK_SIZE)?;

        let mkfs_opts = MkfsOptions::default().with_block_size(EXT4_BLOCK_SIZE);
        ext4_lwext4::mkfs(device, &mkfs_opts).context("lwext4 mkfs failed")?;
        store
            .close()
            .await
            .context("close after lwext4 mkfs failed")?;
        Ok(())
    }

    pub async fn open(store: Arc<Store<S>>) -> anyhow::Result<Self> {
        let rt = tokio::runtime::Handle::current();
        let device = StoreBlockDevice::with_ext4_block_size(
            Arc::clone(&store),
            rt.clone(),
            EXT4_BLOCK_SIZE,
        )?;
        let fs = Ext4Fs::mount(device, false).context("mounting lwext4 filesystem")?;
        Ok(Self {
            fs: Mutex::new(fs),
            store,
            rt,
        })
    }

    pub fn with_fs<R>(&self, f: impl FnOnce(&Ext4Fs) -> anyhow::Result<R>) -> anyhow::Result<R> {
        let guard = self
            .fs
            .lock()
            .map_err(|_| anyhow::anyhow!("lwext4 mutex poisoned"))?;
        f(&guard)
    }

    pub fn store(&self) -> &Arc<Store<S>> {
        &self.store
    }

    pub fn sync_all(&self) -> anyhow::Result<()> {
        {
            let guard = self
                .fs
                .lock()
                .map_err(|_| anyhow::anyhow!("lwext4 mutex poisoned"))?;
            guard.sync().context("lwext4 sync failed")?;
        }
        self.rt
            .block_on(self.store.flush())
            .context("store flush failed")
    }

    pub fn snapshot(&self, new_store_id: String) -> anyhow::Result<()> {
        self.sync_all()?;
        self.rt
            .block_on(self.store.snapshot(new_store_id))
            .context("snapshot failed")
    }

    pub fn clone_store(&self, continuation_id: String, clone_id: String) -> anyhow::Result<()> {
        self.sync_all()?;
        self.rt
            .block_on(self.store.clone_store(continuation_id, clone_id))
            .context("clone failed")
    }
}
