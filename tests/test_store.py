"""Tests for low-level store commands: format, FUSE mount layout."""

import os

from helpers import FUSE_MOUNT, store_format, unique_store_id


def test_format_creates_store():
    sid = unique_store_id("fmt")
    store_format(sid)


def test_format_custom_block_size():
    sid = unique_store_id("fmt-bs")
    store_format(sid, block_size="1M", volume_size="64M")


def test_format_large_volume():
    sid = unique_store_id("fmt-lg")
    store_format(sid, volume_size="10G")


def test_volume_file_exists(fuse):
    store_format(sid := unique_store_id("layout"))
    fuse(sid)
    assert os.path.isfile(f"{FUSE_MOUNT}/volume")


def test_rpc_file_exists(fuse):
    store_format(sid := unique_store_id("layout-rpc"))
    fuse(sid)
    assert os.path.isfile(f"{FUSE_MOUNT}/.loophole/rpc")


def test_volume_size_matches(fuse):
    store_format(sid := unique_store_id("layout-sz"))
    fuse(sid)
    assert os.path.getsize(f"{FUSE_MOUNT}/volume") == 1024 * 1024 * 1024


def test_loophole_dir_exists(fuse):
    store_format(sid := unique_store_id("layout-dir"))
    fuse(sid)
    assert os.path.isdir(f"{FUSE_MOUNT}/.loophole")


def test_root_listing(fuse):
    store_format(sid := unique_store_id("layout-ls"))
    fuse(sid)
    entries = set(os.listdir(FUSE_MOUNT))
    assert "volume" in entries
    assert ".loophole" in entries
