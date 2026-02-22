"""Shared helpers for e2e tests."""

import hashlib
import os
import signal
import subprocess
import sys
import time

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://s3:9000")
BUCKET = os.environ.get("BUCKET", "testbucket")
SHARD = os.environ.get("SHARD", "")
CACHE_DIR = f"/tmp/loophole-cache{SHARD}"
FUSE_MOUNT = f"/mnt/loophole{SHARD}"
EXT4_MOUNT = f"/mnt/ext4{SHARD}"
EXT4_VERIFY = f"/mnt/ext4-verify{SHARD}"

LOOPHOLE = ["loophole", "--bucket", BUCKET, "--endpoint-url", S3_ENDPOINT]

_s3_ready = False


def run(cmd, **kwargs):
    print(f"  $ {cmd if isinstance(cmd, str) else ' '.join(cmd)}")
    return subprocess.run(cmd, shell=isinstance(cmd, str), check=True, **kwargs)


def run_quiet(cmd):
    return subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True)


def s3cmd_prefix():
    host = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    access = os.environ["AWS_ACCESS_KEY_ID"]
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    return f"s3cmd --host={host} --host-bucket= --access_key={access} --secret_key={secret} --no-ssl"


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def unique_store_id(tag):
    return f"{tag}-{int(time.time())}-{os.getpid()}"


def wait_for_s3():
    global _s3_ready
    if _s3_ready:
        return
    prefix = s3cmd_prefix()
    for _ in range(60):
        if run_quiet(f"{prefix} ls s3://").returncode == 0:
            _s3_ready = True
            return
        time.sleep(1)
    sys.exit("S3 did not become ready after 60s")


def ensure_bucket():
    prefix = s3cmd_prefix()
    run_quiet(f"{prefix} mb s3://{BUCKET}")


def wait_for_mountpoint(path, timeout=15, expect_file=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if subprocess.run(
            ["mountpoint", "-q", path], capture_output=True
        ).returncode == 0:
            if expect_file is None or os.path.exists(expect_file):
                return True
        time.sleep(0.5)
    return False


class FuseMount:
    """Manages a low-level FUSE store mount lifecycle."""

    def __init__(self, store_id, mountpoint=FUSE_MOUNT, cache_dir=CACHE_DIR):
        self.store_id = store_id
        self.mountpoint = mountpoint
        self.cache_dir = cache_dir
        self.proc = None

    def start(self):
        os.makedirs(self.mountpoint, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        env = {**os.environ, "RUST_LOG": "info"}
        self.proc = subprocess.Popen(
            LOOPHOLE + [
                "store", "mount",
                "--store", self.store_id,
                "--cache-dir", self.cache_dir,
                "--allow-other", self.mountpoint,
            ],
            env=env,
        )
        volume = os.path.join(self.mountpoint, "volume")
        if not wait_for_mountpoint(self.mountpoint, expect_file=volume):
            if self.proc.poll() is not None:
                raise RuntimeError("FUSE process exited early")
            raise RuntimeError(f"FUSE mount did not appear at {self.mountpoint}")

    def stop(self):
        if self.proc is None:
            return
        run_quiet(f"umount {self.mountpoint}")
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=10)
        self.proc = None


class LoopExt4:
    """Manages a loopback + ext4 mount on top of a FUSE volume file."""

    def __init__(self, volume_path, ext4_mount=EXT4_MOUNT):
        self.volume_path = volume_path
        self.ext4_mount = ext4_mount
        self.loop_dev = None

    def setup(self, format_fs=True):
        os.makedirs(self.ext4_mount, exist_ok=True)
        result = subprocess.run(
            ["losetup", "--find", "--show", self.volume_path],
            capture_output=True, text=True, check=True,
        )
        self.loop_dev = result.stdout.strip()
        print(f"  loop device: {self.loop_dev}")
        if format_fs:
            run(f"mkfs.ext4 -q {self.loop_dev}")
        run(f"mount {self.loop_dev} {self.ext4_mount}")

    def teardown(self):
        if self.loop_dev is None:
            return
        run_quiet(f"umount {self.ext4_mount}")
        run_quiet(f"losetup -d {self.loop_dev}")
        self.loop_dev = None


class HighLevelMount:
    """Manages a high-level `loophole mount` (FUSE + loopback + ext4)."""

    def __init__(self, store_id, mountpoint=EXT4_MOUNT, cache_dir=CACHE_DIR):
        self.store_id = store_id
        self.mountpoint = mountpoint
        self.cache_dir = cache_dir
        self.proc = None

    def start(self):
        os.makedirs(self.mountpoint, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        env = {**os.environ, "RUST_LOG": "info"}
        self.proc = subprocess.Popen(
            LOOPHOLE + [
                "mount",
                "--store", self.store_id,
                "--cache-dir", self.cache_dir,
                self.mountpoint,
            ],
            env=env,
        )
        if not wait_for_mountpoint(self.mountpoint):
            if self.proc.poll() is not None:
                raise RuntimeError("loophole mount process exited early")
            raise RuntimeError(f"ext4 mount did not appear at {self.mountpoint}")

    def stop(self):
        if self.proc is None:
            return
        # Send SIGINT which triggers the cleanup handler.
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=5)
        self.proc = None
        # Clean up any leftover mounts.
        run_quiet(f"umount {self.mountpoint}")


def setup_s3():
    """Ensure S3 is ready, bucket exists, and stale mounts are cleaned up."""
    wait_for_s3()
    ensure_bucket()
    cleanup_mounts()


def store_format(store_id, block_size="4M", volume_size="1G"):
    run(LOOPHOLE + ["store", "format", "--store", store_id,
                     "--block-size", block_size, "--volume-size", volume_size])


def high_level_format(store_id, block_size="4M", volume_size="1G"):
    """Create a store AND format ext4 on the volume (high-level command)."""
    run(LOOPHOLE + ["format", "--store", store_id,
                     "--block-size", block_size, "--volume-size", volume_size])


def cleanup_mounts():
    """Clean up stale mounts and loop devices from previous test runs."""
    for path in [EXT4_MOUNT, FUSE_MOUNT, EXT4_VERIFY]:
        run_quiet(f"umount {path}")
    # Only detach all loop devices when not running in sharded mode,
    # since losetup -D is global and would tear down other shards' devices.
    if not SHARD:
        run_quiet("losetup -D")


def write_test_files(ext4_mount=EXT4_MOUNT):
    """Write a standard set of test files to the ext4 mount."""
    with open(f"{ext4_mount}/greeting.txt", "w") as f:
        f.write("hello from loophole\n")
    run(f"dd if=/dev/urandom of={ext4_mount}/random.bin bs=1M count=10 status=none")
    os.makedirs(f"{ext4_mount}/subdir/nested", exist_ok=True)
    with open(f"{ext4_mount}/subdir/nested/deep.txt", "w") as f:
        f.write("nested file\n")
    with open(f"{ext4_mount}/numbers.txt", "w") as f:
        for i in range(1, 1001):
            f.write(f"{i}\n")
    run("sync")
    return md5(f"{ext4_mount}/random.bin")


def s3_object_size(store_id, block_idx):
    """Return the size of a block's S3 object, or None if missing.
    Returns 0 for tombstones, >0 for real data."""
    prefix = s3cmd_prefix()
    key = f"s3://{BUCKET}/stores/{store_id}/{block_idx:016x}"
    result = subprocess.run(
        f"{prefix} info {key}",
        shell=True, capture_output=True, text=True,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("File size:"):
            return int(line.split(":")[1].strip())
    return None


def s3_object_exists(store_id, block_idx):
    """Check if a block's S3 object exists."""
    return s3_object_size(store_id, block_idx) is not None


def verify_test_files(random_md5, ext4_mount=EXT4_MOUNT):
    """Assert the standard test files are intact."""
    with open(f"{ext4_mount}/greeting.txt") as f:
        assert f.read() == "hello from loophole\n"
    assert md5(f"{ext4_mount}/random.bin") == random_md5
    with open(f"{ext4_mount}/subdir/nested/deep.txt") as f:
        assert f.read() == "nested file\n"
    with open(f"{ext4_mount}/numbers.txt") as f:
        assert len(f.readlines()) == 1000
