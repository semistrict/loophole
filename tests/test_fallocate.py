"""Tests for fallocate punch hole and getxattr support."""

import ctypes
import ctypes.util
import os

from helpers import (
    EXT4_MOUNT, FUSE_MOUNT, FuseMount, LOOPHOLE, LoopExt4,
    run, store_format, unique_store_id,
    s3_object_exists, s3_object_size,
)

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
FALLOC_FL_KEEP_SIZE = 1
FALLOC_FL_PUNCH_HOLE = 2


def _punch_hole(fd, offset, length):
    ret = _libc.fallocate(
        fd,
        FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
        ctypes.c_longlong(offset),
        ctypes.c_longlong(length),
    )
    assert ret == 0, f"fallocate failed, errno={ctypes.get_errno()}"


def test_punch_hole_zeros_data(fuse, ext4):
    """Write data, punch a hole, verify zeros read back."""
    store_id = unique_store_id("punch-zeros")
    store_format(store_id, block_size="4K", volume_size="16M")

    fuse(store_id)
    ext4(f"{FUSE_MOUNT}/volume")

    path = f"{EXT4_MOUNT}/testfile"
    with open(path, "wb") as f:
        f.write(b"A" * 8192)
    run("sync")

    fd = os.open(path, os.O_RDWR)
    try:
        _punch_hole(fd, 4096, 4096)
    finally:
        os.close(fd)
    run("sync")

    with open(path, "rb") as f:
        content = f.read()
    assert len(content) == 8192
    assert content[:4096] == b"A" * 4096
    assert content[4096:8192] == b"\x00" * 4096


def test_punch_hole_creates_tombstone():
    """Write data to parent, snapshot, punch hole in child -- verify tombstone."""
    parent_id = unique_store_id("punch-tomb-p")
    child_id = unique_store_id("punch-tomb-c")
    store_format(parent_id, block_size="4K", volume_size="16M")

    f1 = FuseMount(parent_id)
    f1.start()
    with open(f"{FUSE_MOUNT}/volume", "r+b") as f:
        f.write(b"X" * 4096)
    run("sync")
    run(LOOPHOLE + ["store", "snapshot", FUSE_MOUNT, "--new-store", child_id])
    f1.stop()

    assert s3_object_exists(parent_id, 0)
    assert s3_object_size(parent_id, 0) > 0

    f2 = FuseMount(child_id)
    f2.start()
    fd = os.open(f"{FUSE_MOUNT}/volume", os.O_RDWR)
    try:
        _punch_hole(fd, 0, 4096)
    finally:
        os.close(fd)
    run("sync")
    f2.stop()

    child_size = s3_object_size(child_id, 0)
    assert child_size is not None, "child should have block 0 (tombstone)"
    assert child_size == 0, "child block 0 should be a 0-byte tombstone"


def test_punch_hole_no_ancestor_deletes_block():
    """Write + flush, punch hole (same store) -- S3 object should be deleted."""
    store_id = unique_store_id("punch-del")
    store_format(store_id, block_size="4K", volume_size="16M")

    f1 = FuseMount(store_id)
    f1.start()
    vol = f"{FUSE_MOUNT}/volume"
    with open(vol, "r+b") as f:
        f.write(b"Y" * 4096)
    run("sync")
    assert s3_object_exists(store_id, 0)

    fd = os.open(vol, os.O_RDWR)
    try:
        _punch_hole(fd, 0, 4096)
    finally:
        os.close(fd)
    run("sync")
    f1.stop()

    assert not s3_object_exists(store_id, 0)


def test_punch_hole_partial_block():
    """Punch hole within a block -- only punched bytes zeroed."""
    store_id = unique_store_id("punch-partial")
    store_format(store_id, block_size="4K", volume_size="16M")

    f1 = FuseMount(store_id)
    f1.start()
    vol = f"{FUSE_MOUNT}/volume"
    with open(vol, "r+b") as f:
        f.write(b"Z" * 4096)
    run("sync")

    fd = os.open(vol, os.O_RDWR)
    try:
        _punch_hole(fd, 1024, 2048)
    finally:
        os.close(fd)
    run("sync")

    with open(vol, "rb") as f:
        block = f.read(4096)
    assert block[:1024] == b"Z" * 1024
    assert block[1024:3072] == b"\x00" * 2048
    assert block[3072:4096] == b"Z" * 1024
    f1.stop()
