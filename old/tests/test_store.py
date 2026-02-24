"""Tests for store commands: format, mount layout."""

import os
import pytest

from helpers import (
    EXT4_MOUNT,
    FUSE_MOUNT,
    IS_LINUX,
    high_level_format,
    store_format,
    unique_store_id,
)

linux_only = pytest.mark.skipif(
    not IS_LINUX, reason="requires Linux (FUSE store mount)"
)

# On Linux the high-level mount uses Kernel mode (FUSE→loop→ext4).
# The .loophole virtual dir lives at the FUSE layer (internal temp dir),
# not at the ext4 mountpoint. These tests only apply to modes where
# .loophole is visible at the user mountpoint (lwext4-fuse, NFS).
needs_lwext4_mount = pytest.mark.skipif(
    IS_LINUX, reason="Kernel mode: .loophole is at FUSE layer, not ext4 mount"
)


# ── Format (cross-platform) ────────────────────────────────────────────


def test_format_creates_store():
    sid = unique_store_id("fmt")
    store_format(sid)


def test_format_custom_block_size():
    sid = unique_store_id("fmt-bs")
    store_format(sid, block_size="1M", volume_size="64M")


def test_format_large_volume():
    sid = unique_store_id("fmt-lg")
    store_format(sid, volume_size="10G")


# ── High-level mount layout ───────────────────────────────────────────


@needs_lwext4_mount
def test_ctl_dirs_exist(hl_mount):
    sid = unique_store_id("ctl-dirs")
    high_level_format(sid)
    hl_mount(sid)
    assert os.path.isdir(f"{EXT4_MOUNT}/.loophole")
    assert os.path.isdir(f"{EXT4_MOUNT}/.loophole/snapshots")
    assert os.path.isdir(f"{EXT4_MOUNT}/.loophole/clones")


@needs_lwext4_mount
def test_loophole_in_listing(hl_mount):
    sid = unique_store_id("ctl-ls")
    high_level_format(sid)
    hl_mount(sid)
    entries = set(os.listdir(EXT4_MOUNT))
    assert ".loophole" in entries


# ── Raw FUSE layout (Linux only) ───────────────────────────────────────


@linux_only
def test_volume_file_exists(fuse):
    store_format(sid := unique_store_id("layout"))
    fuse(sid)
    assert os.path.isfile(f"{FUSE_MOUNT}/{sid}")


@linux_only
def test_volume_size_matches(fuse):
    store_format(sid := unique_store_id("layout-sz"))
    fuse(sid)
    assert os.path.getsize(f"{FUSE_MOUNT}/{sid}") == 1024 * 1024 * 1024


@linux_only
def test_fuse_root_listing(fuse):
    store_format(sid := unique_store_id("layout-ls"))
    fuse(sid)
    entries = set(os.listdir(FUSE_MOUNT))
    assert sid in entries
    assert ".loophole" in entries
