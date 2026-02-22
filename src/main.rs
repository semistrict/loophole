#[macro_use]
mod assert;
mod cache;
mod cache_repo;
mod fs;
mod metrics;
mod rpc;
mod s3;
mod store;
mod uploader;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
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

        /// Store ID for the continuation (same lineage)
        #[arg(long)]
        continuation: String,

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

    /// Snapshot via RPC to a running FUSE mount
    Snapshot {
        /// Mount point of the running FUSE filesystem
        mountpoint: PathBuf,

        /// New store ID for the writable child
        #[arg(long)]
        new_store: String,
    },

    /// Clone via RPC to a running FUSE mount
    Clone {
        /// Mount point of the running FUSE filesystem
        mountpoint: PathBuf,

        /// Store ID for the continuation (same lineage)
        #[arg(long)]
        continuation: String,

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

async fn s3_client(endpoint_url: Option<&str>) -> aws_sdk_s3::Client {
    let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
    if let Some(url) = endpoint_url {
        s3_config = s3_config.endpoint_url(url).force_path_style(true);
    }
    aws_sdk_s3::Client::from_conf(s3_config.build())
}

/// Given an ext4 mountpoint, find the FUSE mount and RPC path.
///
/// Walk: ext4 mountpoint → loop device → backing file (FUSE volume) → FUSE mount → .loophole/rpc
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

    // 3. The backing file should be <fuse_mount>/volume. Go up to find .loophole/rpc.
    let fuse_mount = backing_file
        .parent()
        .ok_or_else(|| anyhow::anyhow!("backing file has no parent: {}", backing_file.display()))?;
    let rpc_path = fuse_mount.join(".loophole").join("rpc");
    anyhow::ensure!(
        rpc_path.exists(),
        "RPC endpoint not found at {} (backing file: {})",
        rpc_path.display(),
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
                // Drain kernel-side buffered writes for this FUSE mount before
                // requesting an immutable metadata transition.
                syncfs_mount(&mountpoint)?;
                let req = rpc::Request::Snapshot {
                    new_store_id: new_store,
                };
                let resp = rpc::call(&mountpoint, &req)?;
                if resp.ok {
                    info!("snapshot created");
                } else {
                    anyhow::bail!("snapshot failed: {}", resp.error.unwrap_or_default());
                }
            }

            StoreCommand::Clone {
                mountpoint,
                continuation,
                clone,
            } => {
                // Drain kernel-side buffered writes for this FUSE mount before
                // requesting an immutable metadata transition.
                syncfs_mount(&mountpoint)?;
                let req = rpc::Request::Clone {
                    continuation_id: continuation,
                    clone_id: clone,
                };
                let resp = rpc::call(&mountpoint, &req)?;
                if resp.ok {
                    info!("clone created");
                } else {
                    anyhow::bail!("clone failed: {}", resp.error.unwrap_or_default());
                }
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
        },

        // =============================================================
        // High-level ext4 commands
        // =============================================================
        Command::Format {
            store: store_id,
            block_size,
            volume_size,
        } => {
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
            let fuse_dir = tempfile::tempdir().context("creating temp FUSE mountpoint")?;
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
                    fuser::mount2(filesystem, &fuse_path, &config).expect("FUSE mount failed");
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

            // Create loop device.
            let output = std::process::Command::new("losetup")
                .args(["--find", "--show"])
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
                .args(["-q", &loop_dev])
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

        Command::Mount {
            store: store_id,
            cache_dir,
            cache_size,
            max_uploads,
            max_downloads,
            mountpoint,
        } => {
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
            let fuse_dir = tempfile::tempdir().context("creating temp FUSE mountpoint")?;
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
                    fuser::mount2(filesystem, &fuse_path, &config).expect("FUSE mount failed");
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

            // 2. Create loop device.
            let output = std::process::Command::new("losetup")
                .args(["--find", "--show"])
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
            let status = std::process::Command::new("mount")
                .args([&loop_dev, &mountpoint.to_string_lossy().to_string()])
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

        Command::Snapshot {
            mountpoint,
            new_store,
        } => {
            // 1. Sync the full stack: ext4 → loop device → FUSE backing file.
            sync_stack(&mountpoint)?;

            // 2. Find the FUSE mount and call snapshot via RPC.
            let fuse_mount = find_fuse_mount(&mountpoint)?;
            info!(fuse_mount = %fuse_mount.display(), "found FUSE mount");

            let req = rpc::Request::Snapshot {
                new_store_id: new_store,
            };
            let resp = rpc::call(&fuse_mount, &req)?;
            if resp.ok {
                info!("snapshot created");
            } else {
                anyhow::bail!("snapshot failed: {}", resp.error.unwrap_or_default());
            }
        }

        Command::Clone {
            mountpoint,
            continuation,
            clone,
        } => {
            // 1. Sync the full stack: ext4 → loop device → FUSE backing file.
            sync_stack(&mountpoint)?;

            // 2. Find the FUSE mount and call clone via RPC.
            let fuse_mount = find_fuse_mount(&mountpoint)?;
            info!(fuse_mount = %fuse_mount.display(), "found FUSE mount");

            let req = rpc::Request::Clone {
                continuation_id: continuation,
                clone_id: clone,
            };
            let resp = rpc::call(&fuse_mount, &req)?;
            if resp.ok {
                info!("clone created");
            } else {
                anyhow::bail!("clone failed: {}", resp.error.unwrap_or_default());
            }
        }
    }

    Ok(())
}
