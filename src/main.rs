use clap::{Parser, Subcommand};
#[cfg(feature = "block-fuse")]
use loophole::fs;
#[cfg(feature = "ext4-fuse")]
use loophole::fs_ext4;
use loophole::{assert, cache, metrics, store};
#[cfg(feature = "lwext4")]
use loophole::{blockdev_adapter, lwext4_api};
use std::path::PathBuf;
#[cfg(feature = "ext4-fuse")]
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(about = "FUSE filesystem backed by S3 with CoW clone support")]
struct Cli {
    /// S3 bucket name
    #[arg(long)]
    bucket: String,

    /// Global key prefix for all stores (e.g. "myproject/volumes")
    #[arg(long, default_value = "")]
    prefix: String,

    /// S3 endpoint URL (for S3-compatible stores like RustFS/MinIO)
    #[arg(long)]
    endpoint_url: Option<String>,

    /// Port for Prometheus metrics HTTP endpoint
    #[arg(long, default_value = "9090")]
    metrics_port: u16,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Low-level store operations (format, mount FUSE, snapshot, clone)
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },

    /// Create a new store and format an ext4 filesystem on it
    Format {
        /// Store ID to create
        #[arg(long)]
        store: String,

        /// Block size, e.g. 4M
        #[arg(long, default_value = "4M", value_parser = parse_size)]
        block_size: u64,

        /// Volume size, e.g. 1G
        #[arg(long, value_parser = parse_size)]
        volume_size: u64,
    },

    /// Mount a store as an ext4 filesystem (FUSE + loopback + ext4)
    Mount {
        /// Store ID to mount
        #[arg(long)]
        store: String,

        /// Local directory for caching blocks
        #[arg(long)]
        cache_dir: PathBuf,

        /// Max cache size, e.g. 10G (0 = unlimited)
        #[arg(long, default_value = "0", value_parser = parse_size)]
        cache_size: u64,

        /// Max concurrent S3 block uploads
        #[arg(long, default_value = "20")]
        max_uploads: usize,

        /// Max concurrent background S3 block fetches
        #[arg(long, default_value = "200")]
        max_downloads: usize,

        /// ext4 mount point
        mountpoint: PathBuf,
    },

    /// Snapshot: sync ext4, flush loopback, then snapshot the store
    Snapshot {
        /// ext4 mount point
        mountpoint: PathBuf,

        /// New store ID for the writable child
        #[arg(long)]
        new_store: String,
    },

    /// Clone: sync ext4, flush loopback, then clone the store
    Clone {
        /// ext4 mount point
        mountpoint: PathBuf,

        /// Store ID for the clone (new branch)
        #[arg(long)]
        clone: String,
    },
}

#[derive(Subcommand, Debug)]
enum StoreCommand {
    /// Create a new store (writes initial state.json to S3)
    Format {
        /// Store ID to create
        #[arg(long)]
        store: String,

        /// Block size, e.g. 4M
        #[arg(long, default_value = "4M", value_parser = parse_size)]
        block_size: u64,

        /// Volume size, e.g. 1G
        #[arg(long, value_parser = parse_size)]
        volume_size: u64,
    },

    /// Snapshot via the running filesystem's control directory
    Snapshot {
        /// Mount point of the running filesystem
        mountpoint: PathBuf,

        /// New store ID for the writable child
        #[arg(long)]
        new_store: String,
    },

    /// Clone via the running filesystem's control directory
    Clone {
        /// Mount point of the running filesystem
        mountpoint: PathBuf,

        /// Store ID for the clone (new branch)
        #[arg(long)]
        clone: String,
    },

    /// Mount an existing store as a FUSE filesystem (low-level)
    Mount {
        /// Store ID to mount
        #[arg(long)]
        store: String,

        /// Local directory for caching blocks
        #[arg(long)]
        cache_dir: PathBuf,

        /// Max cache size, e.g. 10G (0 = unlimited)
        #[arg(long, default_value = "0", value_parser = parse_size)]
        cache_size: u64,

        /// Max concurrent S3 block uploads
        #[arg(long, default_value = "20")]
        max_uploads: usize,

        /// Max concurrent background S3 block fetches
        #[arg(long, default_value = "200")]
        max_downloads: usize,

        /// Allow other users (and the kernel) to access the mount
        #[arg(long)]
        allow_other: bool,

        /// Local mount point
        mountpoint: PathBuf,
    },
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s == "0" {
        return Ok(0);
    }
    let (num, mult) = match s.chars().last() {
        Some('K') | Some('k') => (&s[..s.len() - 1], 1024u64),
        Some('M') | Some('m') => (&s[..s.len() - 1], 1024 * 1024),
        Some('G') | Some('g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        Some('T') | Some('t') => (&s[..s.len() - 1], 1024u64 * 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    let n: u64 = num.parse().map_err(|_| format!("invalid size: {s}"))?;
    n.checked_mul(mult)
        .ok_or_else(|| format!("size overflows u64: {s}"))
}

#[derive(Clone, Copy, Debug)]
enum HighLevelMode {
    Fuse,
    Lwext4,
    Nfs,
}

fn high_level_mode() -> anyhow::Result<HighLevelMode> {
    const HAS_FUSE: bool = cfg!(feature = "block-fuse");
    const HAS_LWEXT4_FUSE: bool = cfg!(feature = "ext4-fuse");
    const HAS_NFS: bool = cfg!(feature = "nfs");

    let requested = std::env::var("LOOPHOLE_MODE").ok();
    if let Some(mode) = requested.as_deref() {
        return match mode {
            "fuse" if HAS_FUSE => Ok(HighLevelMode::Fuse),
            "kernel" if HAS_FUSE => {
                tracing::warn!("LOOPHOLE_MODE=kernel is deprecated, use LOOPHOLE_MODE=fuse");
                Ok(HighLevelMode::Fuse)
            }
            "lwext4" | "lwext4-fuse" if HAS_LWEXT4_FUSE => Ok(HighLevelMode::Lwext4),
            "nfs" if HAS_NFS => Ok(HighLevelMode::Nfs),
            "fuse" | "kernel" => {
                anyhow::bail!("LOOPHOLE_MODE={mode} requested but fuse mode is not compiled in")
            }
            "lwext4" | "lwext4-fuse" => {
                anyhow::bail!(
                    "LOOPHOLE_MODE={mode} requested but lwext4+fuse mode is not compiled in"
                )
            }
            "nfs" => {
                anyhow::bail!("LOOPHOLE_MODE=nfs requested but nfs feature is not compiled in")
            }
            other => anyhow::bail!(
                "unknown LOOPHOLE_MODE={other:?}; expected one of: fuse, lwext4, lwext4-fuse, nfs"
            ),
        };
    }

    // Auto-detect: on macOS prefer NFS (no FUSE needed), otherwise fuse > lwext4
    if cfg!(target_os = "macos") && HAS_NFS {
        return Ok(HighLevelMode::Nfs);
    }

    anyhow::ensure!(
        HAS_FUSE || HAS_LWEXT4_FUSE || HAS_NFS,
        "no high-level mount mode compiled (enable fuse-mode, lwext4+fuse, or nfs)"
    );

    if HAS_FUSE && HAS_LWEXT4_FUSE {
        if cfg!(target_os = "macos") {
            Ok(HighLevelMode::Lwext4)
        } else {
            Ok(HighLevelMode::Fuse)
        }
    } else if HAS_LWEXT4_FUSE {
        Ok(HighLevelMode::Lwext4)
    } else if HAS_NFS {
        Ok(HighLevelMode::Nfs)
    } else {
        Ok(HighLevelMode::Fuse)
    }
}

async fn s3_client(endpoint_url: Option<&str>) -> aws_sdk_s3::Client {
    let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
    if let Some(url) = endpoint_url {
        s3_config = s3_config.endpoint_url(url).force_path_style(true);
    }
    aws_sdk_s3::Client::from_conf(s3_config.build())
}

/// Given an ext4 mountpoint, find the underlying FUSE mount.
///
/// Walk: ext4 mountpoint → loop device → backing file (FUSE volume) → FUSE mount
fn find_fuse_mount(ext4_mountpoint: &std::path::Path) -> anyhow::Result<PathBuf> {
    use std::process::Command as Cmd;

    let ext4_mountpoint = ext4_mountpoint.canonicalize().with_context(|| {
        format!(
            "canonicalizing ext4 mountpoint {}",
            ext4_mountpoint.display()
        )
    })?;

    // 1. Find the device backing the ext4 mount via findmnt.
    let output = Cmd::new("findmnt")
        .args(["-n", "-o", "SOURCE", "--target"])
        .arg(&ext4_mountpoint)
        .output()
        .context("running findmnt")?;
    anyhow::ensure!(
        output.status.success(),
        "findmnt failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let device = String::from_utf8(output.stdout)
        .context("findmnt output not UTF-8")?
        .trim()
        .to_string();
    anyhow::ensure!(
        device.starts_with("/dev/loop"),
        "ext4 mount at {} is not on a loop device (found: {})",
        ext4_mountpoint.display(),
        device
    );

    // 2. Find the backing file of the loop device via losetup.
    let output = Cmd::new("losetup")
        .args(["--noheadings", "--output", "BACK-FILE"])
        .arg(&device)
        .output()
        .context("running losetup")?;
    anyhow::ensure!(
        output.status.success(),
        "losetup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let backing_file = PathBuf::from(
        String::from_utf8(output.stdout)
            .context("losetup output not UTF-8")?
            .trim(),
    );

    // 3. The backing file should be <fuse_mount>/volume.
    let fuse_mount = backing_file
        .parent()
        .ok_or_else(|| anyhow::anyhow!("backing file has no parent: {}", backing_file.display()))?;
    let ctl_dir = fuse_mount.join(".loophole");
    anyhow::ensure!(
        ctl_dir.exists(),
        ".loophole control dir not found at {} (backing file: {})",
        ctl_dir.display(),
        backing_file.display()
    );

    Ok(fuse_mount.to_path_buf())
}

/// Sync all layers: ext4 → loop device → FUSE.
///
/// 1. syncfs on the ext4 mount (flushes ext4 journal + dirty pages)
/// 2. fsync the loop device (flushes loop writeback to the backing file)
/// 3. The FUSE layer sees the writes and the store's own flush handles S3 upload
fn syncfs_mount(mountpoint: &std::path::Path) -> anyhow::Result<()> {
    info!(mountpoint = %mountpoint.display(), "syncing mount");

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(mountpoint)
            .with_context(|| format!("opening {} for syncfs", mountpoint.display()))?;
        let ret = unsafe { libc::syscall(libc::SYS_syncfs, f.as_raw_fd()) };
        anyhow::ensure!(
            ret == 0,
            "syncfs failed: {}",
            std::io::Error::last_os_error()
        );
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Fallback: global sync (less targeted but correct).
        unsafe { libc::sync() };
    }

    Ok(())
}

fn sync_stack(ext4_mountpoint: &std::path::Path) -> anyhow::Result<()> {
    use std::process::Command as Cmd;

    info!(mountpoint = %ext4_mountpoint.display(), "syncing ext4");

    // 1. syncfs on ext4.
    syncfs_mount(ext4_mountpoint)?;

    // 2. Find and fsync the loop device to push data through to the backing file.
    let output = Cmd::new("findmnt")
        .args(["-n", "-o", "SOURCE", "--target"])
        .arg(ext4_mountpoint)
        .output()
        .context("running findmnt for loop device")?;
    let device = String::from_utf8(output.stdout)
        .context("findmnt output not UTF-8")?
        .trim()
        .to_string();
    if device.starts_with("/dev/loop") {
        info!(device = %device, "fsyncing loop device");
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&device)
            .with_context(|| format!("opening loop device {device}"))?;
        f.sync_all()
            .with_context(|| format!("fsync loop device {device}"))?;
    }

    info!("stack sync complete");
    Ok(())
}

use anyhow::Context;

/// Trigger a snapshot by creating `.loophole/snapshots/<name>` and reading back status.
fn trigger_snapshot(mount: &std::path::Path, name: &str) -> anyhow::Result<()> {
    let ctl_path = mount.join(".loophole/snapshots").join(name);
    std::fs::write(&ctl_path, b"").with_context(|| format!("creating {}", ctl_path.display()))?;
    let status = std::fs::read_to_string(&ctl_path)
        .with_context(|| format!("reading {}", ctl_path.display()))?;
    info!(status = %status, "snapshot result");
    check_ctl_status(&status, "snapshot")
}

/// Trigger a clone by creating `.loophole/clones/<name>` and reading back status.
fn trigger_clone(mount: &std::path::Path, name: &str) -> anyhow::Result<()> {
    let ctl_path = mount.join(".loophole/clones").join(name);
    std::fs::write(&ctl_path, b"").with_context(|| format!("creating {}", ctl_path.display()))?;
    let status = std::fs::read_to_string(&ctl_path)
        .with_context(|| format!("reading {}", ctl_path.display()))?;
    info!(status = %status, "clone result");
    check_ctl_status(&status, "clone")
}

fn check_ctl_status(status_json: &str, op: &str) -> anyhow::Result<()> {
    let parsed: serde_json::Value =
        serde_json::from_str(status_json).with_context(|| format!("parsing {op} status JSON"))?;
    match parsed.get("status").and_then(|v| v.as_str()) {
        Some("complete") => Ok(()),
        Some("error") => {
            let err = parsed
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            anyhow::bail!("{op} failed: {err}");
        }
        other => anyhow::bail!("{op} returned unexpected status: {other:?}"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    assert::init();

    let cli = Cli::parse();
    metrics::init(cli.metrics_port);

    match cli.command {
        // =============================================================
        // Low-level store commands
        // =============================================================
        Command::Store { command } => match command {
            StoreCommand::Format {
                store: store_id,
                block_size,
                volume_size,
            } => {
                let client = s3_client(cli.endpoint_url.as_deref()).await;
                store::Store::format(
                    &client,
                    &cli.bucket,
                    &cli.prefix,
                    &store_id,
                    block_size,
                    volume_size,
                )
                .await?;
            }

            StoreCommand::Snapshot {
                mountpoint,
                new_store,
            } => {
                syncfs_mount(&mountpoint)?;
                trigger_snapshot(&mountpoint, &new_store)?;
            }

            StoreCommand::Clone { mountpoint, clone } => {
                syncfs_mount(&mountpoint)?;
                trigger_clone(&mountpoint, &clone)?;
            }

            StoreCommand::Mount {
                store: store_id,
                cache_dir,
                cache_size,
                max_uploads,
                max_downloads,
                allow_other,
                mountpoint,
            } => {
                #[cfg(feature = "block-fuse")]
                {
                    let client = s3_client(cli.endpoint_url.as_deref()).await;
                    cache::init(cache_dir.clone(), cache_size).await?;

                    let store = store::Store::load(
                        client,
                        cli.bucket.clone(),
                        cli.prefix.clone(),
                        store_id.clone(),
                        max_uploads,
                        max_downloads,
                    )
                    .await?;

                    info!(
                        store = %store_id,
                        mountpoint = %mountpoint.display(),
                        cache_dir = %cache_dir.display(),
                        "mounting FUSE"
                    );

                    let filesystem = fs::Fs::new(store);

                    let mut config = fuser::Config::default();
                    if allow_other {
                        config.acl = fuser::SessionACL::All;
                    }

                    tokio::task::spawn_blocking(move || {
                        fuser::mount2(filesystem, &mountpoint, &config).expect("FUSE mount failed");
                    })
                    .await?;
                }
                #[cfg(not(feature = "block-fuse"))]
                {
                    let _ = (
                        store_id,
                        cache_dir,
                        cache_size,
                        max_uploads,
                        max_downloads,
                        allow_other,
                        mountpoint,
                    );
                    anyhow::bail!("store mount requires the block-fuse feature");
                }
            }
        },

        // =============================================================
        // High-level ext4 commands
        // =============================================================
        Command::Format {
            store: store_id,
            block_size,
            volume_size,
        } => {
            match high_level_mode()? {
                HighLevelMode::Fuse => {
                    #[cfg(feature = "block-fuse")]
                    {
                        let client = s3_client(cli.endpoint_url.as_deref()).await;

                        // 1. Create the store in S3.
                        store::Store::format(
                            &client,
                            &cli.bucket,
                            &cli.prefix,
                            &store_id,
                            block_size,
                            volume_size,
                        )
                        .await?;

                        info!(store = %store_id, "store created, formatting ext4 on volume");

                        // 2. Mount FUSE in a temp directory, create loopback, mkfs.ext4.
                        let fuse_dir =
                            tempfile::tempdir().context("creating temp FUSE mountpoint")?;
                        let cache_dir = tempfile::tempdir().context("creating temp cache dir")?;
                        cache::init(cache_dir.path().to_path_buf(), 0).await?;

                        let store = store::Store::load(
                            client,
                            cli.bucket.clone(),
                            cli.prefix.clone(),
                            store_id.clone(),
                            20,
                            200,
                        )
                        .await?;

                        let filesystem = fs::Fs::new(store);
                        let fuse_path = fuse_dir.path().to_path_buf();
                        let mut config = fuser::Config::default();
                        config.acl = fuser::SessionACL::All;

                        let fuse_handle = tokio::task::spawn_blocking({
                            let fuse_path = fuse_path.clone();
                            move || {
                                fuser::mount2(filesystem, &fuse_path, &config)
                                    .expect("FUSE mount failed");
                            }
                        });

                        // Wait for FUSE to appear.
                        for _ in 0..30 {
                            if std::process::Command::new("mountpoint")
                                .arg("-q")
                                .arg(&fuse_path)
                                .status()
                                .map(|s| s.success())
                                .unwrap_or(false)
                            {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }

                        let volume_path = fuse_path.join("volume");
                        anyhow::ensure!(volume_path.exists(), "FUSE volume file not found");

                        // Create loop device with direct I/O to bypass kernel page cache.
                        let output = std::process::Command::new("losetup")
                            .args(["--find", "--show", "--direct-io=on"])
                            .arg(&volume_path)
                            .output()
                            .context("running losetup")?;
                        anyhow::ensure!(
                            output.status.success(),
                            "losetup failed: {}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                        let loop_dev = String::from_utf8(output.stdout)
                            .context("losetup output not UTF-8")?
                            .trim()
                            .to_string();
                        info!(loop_device = %loop_dev, "created loop device");

                        // mkfs.ext4
                        let status = std::process::Command::new("mkfs.ext4")
                            .args([
                                "-q",
                                "-m",
                                "0",
                                "-E",
                                "lazy_itable_init=1,lazy_journal_init=1",
                                &loop_dev,
                            ])
                            .status()
                            .context("running mkfs.ext4")?;
                        anyhow::ensure!(status.success(), "mkfs.ext4 failed");
                        info!("ext4 filesystem created");

                        // Sync and tear down.
                        let _ = std::process::Command::new("sync").status();
                        let _ = std::process::Command::new("losetup")
                            .args(["-d", &loop_dev])
                            .status();
                        let _ = std::process::Command::new("umount")
                            .arg(&fuse_path)
                            .status();
                        let _ = fuse_handle.await;

                        info!(store = %store_id, "format complete — store has empty ext4 filesystem");
                    }
                    #[cfg(not(feature = "block-fuse"))]
                    {
                        anyhow::bail!("fuse mode is not compiled in");
                    }
                }
                HighLevelMode::Lwext4 | HighLevelMode::Nfs => {
                    #[cfg(feature = "lwext4")]
                    {
                        let client = s3_client(cli.endpoint_url.as_deref()).await;
                        store::Store::format(
                            &client,
                            &cli.bucket,
                            &cli.prefix,
                            &store_id,
                            block_size,
                            volume_size,
                        )
                        .await?;

                        // Formatting writes through Store, which requires a cache root.
                        let cache_dir = tempfile::tempdir().context("creating temp cache dir")?;
                        cache::init(cache_dir.path().to_path_buf(), 0).await?;
                        let store = store::Store::load(
                            client,
                            cli.bucket.clone(),
                            cli.prefix.clone(),
                            store_id.clone(),
                            20,
                            200,
                        )
                        .await?;
                        lwext4_api::Lwext4Volume::format(store).await?;
                        info!(store = %store_id, "format complete — store has empty lwext4 filesystem");
                    }
                    #[cfg(not(feature = "lwext4"))]
                    {
                        anyhow::bail!("lwext4 feature is not compiled in");
                    }
                }
            }
        }

        Command::Mount {
            store: store_id,
            cache_dir,
            cache_size,
            max_uploads,
            max_downloads,
            mountpoint,
        } => {
            match high_level_mode()? {
                HighLevelMode::Fuse => {
                    #[cfg(feature = "block-fuse")]
                    {
                        let client = s3_client(cli.endpoint_url.as_deref()).await;
                        cache::init(cache_dir.clone(), cache_size).await?;

                        let store = store::Store::load(
                            client,
                            cli.bucket.clone(),
                            cli.prefix.clone(),
                            store_id.clone(),
                            max_uploads,
                            max_downloads,
                        )
                        .await?;

                        // 1. Mount FUSE in a temporary directory (not under the ext4 mountpoint,
                        //    because ext4 would shadow it and make .loophole/rpc inaccessible).
                        let fuse_dir =
                            tempfile::tempdir().context("creating temp FUSE mountpoint")?;
                        let fuse_path = fuse_dir.path().to_path_buf();
                        std::fs::create_dir_all(&mountpoint).context("creating mountpoint")?;

                        info!(
                            store = %store_id,
                            mountpoint = %mountpoint.display(),
                            fuse_dir = %fuse_path.display(),
                            "mounting full stack"
                        );

                        let filesystem = fs::Fs::new(store);
                        let mut config = fuser::Config::default();
                        config.acl = fuser::SessionACL::All;

                        let fuse_handle = tokio::task::spawn_blocking({
                            let fuse_path = fuse_path.clone();
                            move || {
                                fuser::mount2(filesystem, &fuse_path, &config)
                                    .expect("FUSE mount failed");
                            }
                        });

                        // Wait for FUSE to appear.
                        for _ in 0..30 {
                            if std::process::Command::new("mountpoint")
                                .arg("-q")
                                .arg(&fuse_path)
                                .status()
                                .map(|s| s.success())
                                .unwrap_or(false)
                            {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                        anyhow::ensure!(
                            std::process::Command::new("mountpoint")
                                .arg("-q")
                                .arg(&fuse_path)
                                .status()
                                .map(|s| s.success())
                                .unwrap_or(false),
                            "FUSE mount did not appear at {}",
                            fuse_path.display()
                        );

                        let volume_path = fuse_path.join("volume");

                        // 2. Create loop device with direct I/O to bypass kernel page cache.
                        let output = std::process::Command::new("losetup")
                            .args(["--find", "--show", "--direct-io=on"])
                            .arg(&volume_path)
                            .output()
                            .context("running losetup")?;
                        anyhow::ensure!(
                            output.status.success(),
                            "losetup failed: {}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                        let loop_dev = String::from_utf8(output.stdout)
                            .context("losetup output not UTF-8")?
                            .trim()
                            .to_string();
                        info!(loop_device = %loop_dev, "created loop device");

                        // 3. Mount ext4.
                        let mp = mountpoint.to_string_lossy();
                        let status = std::process::Command::new("mount")
                            .args([
                                "-o",
                                "noatime,data=writeback,nobarrier",
                                loop_dev.as_str(),
                                mp.as_ref(),
                            ])
                            .status()
                            .context("mounting ext4")?;
                        anyhow::ensure!(status.success(), "ext4 mount failed");
                        info!(mountpoint = %mountpoint.display(), "ext4 mounted");

                        // 4. Wait for FUSE to exit (which happens when we unmount).
                        //    On SIGTERM/SIGINT, clean up in reverse order.
                        let mountpoint_clone = mountpoint.clone();
                        let loop_dev_clone = loop_dev.clone();
                        let fuse_path_clone = fuse_path.clone();
                        tokio::spawn(async move {
                            tokio::signal::ctrl_c().await.ok();
                            info!("received signal, tearing down");
                            let _ = std::process::Command::new("umount")
                                .arg(&mountpoint_clone)
                                .status();
                            let _ = std::process::Command::new("losetup")
                                .args(["-d", &loop_dev_clone])
                                .status();
                            let _ = std::process::Command::new("umount")
                                .arg(&fuse_path_clone)
                                .status();
                        });

                        fuse_handle.await?;
                        // Keep fuse_dir alive until after FUSE exits so the tempdir isn't removed early.
                        drop(fuse_dir);
                    }
                    #[cfg(not(feature = "block-fuse"))]
                    {
                        anyhow::bail!("fuse mode is not compiled in");
                    }
                }
                HighLevelMode::Lwext4 => {
                    #[cfg(feature = "ext4-fuse")]
                    {
                        let client = s3_client(cli.endpoint_url.as_deref()).await;
                        cache::init(cache_dir.clone(), cache_size).await?;
                        let store = store::Store::load(
                            client,
                            cli.bucket.clone(),
                            cli.prefix.clone(),
                            store_id.clone(),
                            max_uploads,
                            max_downloads,
                        )
                        .await?;

                        std::fs::create_dir_all(&mountpoint).context("creating mountpoint")?;
                        info!(
                            store = %store_id,
                            mountpoint = %mountpoint.display(),
                            mode = "lwext4",
                            "mounting lwext4 FUSE"
                        );

                        let device = blockdev_adapter::StoreBlockDevice::new(
                            Arc::clone(&store),
                            tokio::runtime::Handle::current(),
                        )?;
                        let ext4_fs = ext4_lwext4::Ext4Fs::mount(device, false)
                            .context("mounting lwext4 in-process filesystem")?;
                        let filesystem = fs_ext4::Ext4Fuse::new(store, ext4_fs);
                        let config = fuser::Config::default();

                        tokio::task::spawn_blocking(move || {
                            fuser::mount2(filesystem, &mountpoint, &config)
                                .expect("FUSE mount failed");
                        })
                        .await?;
                    }
                    #[cfg(not(feature = "ext4-fuse"))]
                    {
                        anyhow::bail!("lwext4 mode is not compiled in");
                    }
                }
                HighLevelMode::Nfs => {
                    #[cfg(feature = "nfs")]
                    {
                        use nfsserve::tcp::{NFSTcp, NFSTcpListener};

                        let client = s3_client(cli.endpoint_url.as_deref()).await;
                        cache::init(cache_dir.clone(), cache_size).await?;
                        let store = store::Store::load(
                            client,
                            cli.bucket.clone(),
                            cli.prefix.clone(),
                            store_id.clone(),
                            max_uploads,
                            max_downloads,
                        )
                        .await?;

                        std::fs::create_dir_all(&mountpoint).context("creating mountpoint")?;

                        // The StoreBlockDevice gets the *main* runtime handle.
                        // NFS handlers call block_in_place + handle.block_on()
                        // to drive store futures. This must target a runtime
                        // whose worker threads are NOT consumed by the NFS
                        // server itself, otherwise all workers deadlock.
                        let device = blockdev_adapter::StoreBlockDevice::new(
                            Arc::clone(&store),
                            tokio::runtime::Handle::current(),
                        )?;
                        let ext4_fs = ext4_lwext4::Ext4Fs::mount(device, false)
                            .context("mounting lwext4 in-process filesystem")?;
                        let nfs_fs = loophole::nfs::Ext4Nfs::new(store, ext4_fs);

                        // Build the NFS listener on the main runtime (for the
                        // TCP bind) then move it to a dedicated runtime.
                        let listener = NFSTcpListener::bind("127.0.0.1:0", nfs_fs)
                            .await
                            .context("binding NFS listener")?;
                        let port = listener.get_listen_port();

                        info!(
                            store = %store_id,
                            mountpoint = %mountpoint.display(),
                            port = port,
                            mode = "nfs",
                            "starting NFS server"
                        );

                        // Run handle_forever() on a *separate* tokio runtime.
                        // NFS handlers call block_in_place which consumes tokio
                        // worker threads. If they share the main runtime, all
                        // workers deadlock because block_on() tries to drive
                        // store futures on the same starved pool.
                        let nfs_rt = tokio::runtime::Builder::new_multi_thread()
                            .enable_all()
                            .thread_name("nfs-server")
                            .build()
                            .context("building NFS runtime")?;

                        let nfs_handle =
                            std::thread::spawn(move || nfs_rt.block_on(listener.handle_forever()));

                        // Mount via mount_nfs on macOS, mount.nfs on Linux
                        let nfs_opts = if cfg!(target_os = "macos") {
                            format!("port={port},mountport={port},vers=3,tcp,resvport,nolocks")
                        } else {
                            format!("port={port},mountport={port},vers=3,tcp,nolock")
                        };
                        let mp = mountpoint.to_string_lossy().to_string();
                        let mount_status = if cfg!(target_os = "macos") {
                            tokio::process::Command::new("sudo")
                                .args(["mount_nfs", "-o", &nfs_opts, "127.0.0.1:/", &mp])
                                .status()
                                .await
                        } else {
                            tokio::process::Command::new("mount")
                                .args(["-t", "nfs", "-o", &nfs_opts, "127.0.0.1:/", &mp])
                                .status()
                                .await
                        }
                        .context("running NFS mount")?;
                        anyhow::ensure!(mount_status.success(), "NFS mount failed");
                        info!(mountpoint = %mountpoint.display(), "NFS mounted");

                        // Handle ctrl-c: unmount then exit
                        tokio::signal::ctrl_c().await.ok();
                        info!("received signal, unmounting NFS");
                        let _ = if cfg!(target_os = "macos") {
                            std::process::Command::new("sudo")
                                .args(["umount", &mountpoint.to_string_lossy()])
                                .status()
                        } else {
                            std::process::Command::new("umount")
                                .arg(&mountpoint)
                                .status()
                        };

                        // Wait for the NFS server thread to finish
                        let _ = nfs_handle.join();
                    }
                    #[cfg(not(feature = "nfs"))]
                    {
                        anyhow::bail!("nfs feature is not compiled in");
                    }
                }
            }
        }

        Command::Snapshot {
            mountpoint,
            new_store,
        } => match high_level_mode()? {
            HighLevelMode::Fuse => {
                sync_stack(&mountpoint)?;
                let fuse_mount = find_fuse_mount(&mountpoint)?;
                info!(fuse_mount = %fuse_mount.display(), "found FUSE mount");
                trigger_snapshot(&fuse_mount, &new_store)?;
            }
            HighLevelMode::Lwext4 | HighLevelMode::Nfs => {
                syncfs_mount(&mountpoint)?;
                trigger_snapshot(&mountpoint, &new_store)?;
            }
        },

        Command::Clone { mountpoint, clone } => match high_level_mode()? {
            HighLevelMode::Fuse => {
                sync_stack(&mountpoint)?;
                let fuse_mount = find_fuse_mount(&mountpoint)?;
                info!(fuse_mount = %fuse_mount.display(), "found FUSE mount");
                trigger_clone(&fuse_mount, &clone)?;
            }
            HighLevelMode::Lwext4 | HighLevelMode::Nfs => {
                syncfs_mount(&mountpoint)?;
                trigger_clone(&mountpoint, &clone)?;
            }
        },
    }

    Ok(())
}
