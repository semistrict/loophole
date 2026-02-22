"""Tests for snapshot -- both low-level store RPC and high-level ext4-aware."""

import os

from helpers import (
    EXT4_MOUNT, EXT4_VERIFY, FUSE_MOUNT, FuseMount, HighLevelMount, LOOPHOLE, LoopExt4,
    high_level_format, run, store_format, unique_store_id,
    verify_test_files, write_test_files,
)


def test_snapshot_preserves_ext4_data():
    parent_id = unique_store_id("snap-parent")
    child_id = unique_store_id("snap-child")
    store_format(parent_id)

    f1 = FuseMount(parent_id)
    f1.start()
    e1 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e1.setup(format_fs=True)
    random_md5 = write_test_files()
    e1.teardown()

    run(LOOPHOLE + ["store", "snapshot", FUSE_MOUNT, "--new-store", child_id])
    f1.stop()

    f2 = FuseMount(child_id)
    f2.start()
    e2 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e2.setup(format_fs=False)
    try:
        verify_test_files(random_md5)
    finally:
        e2.teardown()
        f2.stop()


def test_child_can_write_independently():
    parent_id = unique_store_id("snap-ind-p")
    child_id = unique_store_id("snap-ind-c")
    store_format(parent_id)

    f1 = FuseMount(parent_id)
    f1.start()
    e1 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e1.setup(format_fs=True)
    with open(f"{EXT4_MOUNT}/parent.txt", "w") as f:
        f.write("from parent\n")
    run("sync")
    e1.teardown()

    run(LOOPHOLE + ["store", "snapshot", FUSE_MOUNT, "--new-store", child_id])
    f1.stop()

    f2 = FuseMount(child_id)
    f2.start()
    e2 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e2.setup(format_fs=False)
    try:
        with open(f"{EXT4_MOUNT}/child.txt", "w") as f:
            f.write("from child\n")
        run("sync")
        with open(f"{EXT4_MOUNT}/parent.txt") as f:
            assert f.read() == "from parent\n"
        with open(f"{EXT4_MOUNT}/child.txt") as f:
            assert f.read() == "from child\n"
    finally:
        e2.teardown()
        f2.stop()


def test_writes_fail_on_frozen_parent():
    parent_id = unique_store_id("snap-frozen")
    child_id = unique_store_id("snap-frozen-c")
    store_format(parent_id)

    f1 = FuseMount(parent_id)
    f1.start()
    e1 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e1.setup(format_fs=True)
    e1.teardown()

    run(LOOPHOLE + ["store", "snapshot", FUSE_MOUNT, "--new-store", child_id])

    try:
        result = os.system(
            f"dd if=/dev/zero of={FUSE_MOUNT}/volume bs=4k count=1 oflag=direct 2>/dev/null"
        )
        assert result != 0, "write to frozen store should fail"
    finally:
        f1.stop()


def test_snapshot_via_ext4_mountpoint():
    parent_id = unique_store_id("hlsnap-p")
    child_id = unique_store_id("hlsnap-c")
    high_level_format(parent_id)

    m1 = HighLevelMount(parent_id)
    m1.start()
    try:
        with open(f"{EXT4_MOUNT}/data.txt", "w") as f:
            f.write("snapshot me\n")
        run("sync")
        run(LOOPHOLE + ["snapshot", EXT4_MOUNT, "--new-store", child_id])
    finally:
        m1.stop()

    f2 = FuseMount(child_id)
    f2.start()
    e2 = LoopExt4(f"{FUSE_MOUNT}/volume", ext4_mount=EXT4_VERIFY)
    e2.setup(format_fs=False)
    try:
        with open(f"{EXT4_VERIFY}/data.txt") as f:
            assert f.read() == "snapshot me\n"
    finally:
        e2.teardown()
        f2.stop()
