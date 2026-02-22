"""Tests for clone -- both low-level store RPC and high-level ext4-aware."""

import os

from helpers import (
    EXT4_MOUNT, EXT4_VERIFY, FUSE_MOUNT, FuseMount, HighLevelMount, LOOPHOLE, LoopExt4,
    high_level_format, run, store_format, unique_store_id,
    verify_test_files, write_test_files,
)


def test_clone_preserves_data_in_both_branches():
    parent_id = unique_store_id("cln-parent")
    continuation_id = unique_store_id("cln-cont")
    clone_id = unique_store_id("cln-clone")
    store_format(parent_id)

    f1 = FuseMount(parent_id)
    f1.start()
    e1 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e1.setup(format_fs=True)
    random_md5 = write_test_files()
    e1.teardown()

    run(LOOPHOLE + ["store", "clone", FUSE_MOUNT,
                     "--continuation", continuation_id,
                     "--clone", clone_id])
    f1.stop()

    # Verify continuation.
    f2 = FuseMount(continuation_id)
    f2.start()
    e2 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e2.setup(format_fs=False)
    try:
        verify_test_files(random_md5)
    finally:
        e2.teardown()
        f2.stop()

    # Verify clone.
    f3 = FuseMount(clone_id)
    f3.start()
    e3 = LoopExt4(f"{FUSE_MOUNT}/volume", ext4_mount=EXT4_VERIFY)
    e3.setup(format_fs=False)
    try:
        verify_test_files(random_md5, ext4_mount=EXT4_VERIFY)
    finally:
        e3.teardown()
        f3.stop()


def test_clone_branches_are_independent():
    parent_id = unique_store_id("cln-ind-p")
    continuation_id = unique_store_id("cln-ind-cont")
    clone_id = unique_store_id("cln-ind-cl")
    store_format(parent_id)

    f1 = FuseMount(parent_id)
    f1.start()
    e1 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e1.setup(format_fs=True)
    with open(f"{EXT4_MOUNT}/shared.txt", "w") as f:
        f.write("from parent\n")
    run("sync")
    e1.teardown()

    run(LOOPHOLE + ["store", "clone", FUSE_MOUNT,
                     "--continuation", continuation_id,
                     "--clone", clone_id])
    f1.stop()

    # Write to continuation.
    f2 = FuseMount(continuation_id)
    f2.start()
    e2 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e2.setup(format_fs=False)
    with open(f"{EXT4_MOUNT}/cont.txt", "w") as f:
        f.write("continuation only\n")
    run("sync")
    e2.teardown()
    f2.stop()

    # Write to clone and verify isolation.
    f3 = FuseMount(clone_id)
    f3.start()
    e3 = LoopExt4(f"{FUSE_MOUNT}/volume")
    e3.setup(format_fs=False)
    try:
        with open(f"{EXT4_MOUNT}/clone.txt", "w") as f:
            f.write("clone only\n")
        run("sync")
        with open(f"{EXT4_MOUNT}/shared.txt") as f:
            assert f.read() == "from parent\n"
        with open(f"{EXT4_MOUNT}/clone.txt") as f:
            assert f.read() == "clone only\n"
        assert not os.path.exists(f"{EXT4_MOUNT}/cont.txt")
    finally:
        e3.teardown()
        f3.stop()


def test_clone_via_ext4_mountpoint():
    parent_id = unique_store_id("hlcln-p")
    continuation_id = unique_store_id("hlcln-cont")
    clone_id = unique_store_id("hlcln-cl")
    high_level_format(parent_id)

    m1 = HighLevelMount(parent_id)
    m1.start()
    try:
        with open(f"{EXT4_MOUNT}/data.txt", "w") as f:
            f.write("clone me\n")
        run("sync")
        run(LOOPHOLE + ["clone", EXT4_MOUNT,
                         "--continuation", continuation_id,
                         "--clone", clone_id])
    finally:
        m1.stop()

    f2 = FuseMount(clone_id)
    f2.start()
    e2 = LoopExt4(f"{FUSE_MOUNT}/volume", ext4_mount=EXT4_VERIFY)
    e2.setup(format_fs=False)
    try:
        with open(f"{EXT4_VERIFY}/data.txt") as f:
            assert f.read() == "clone me\n"
    finally:
        e2.teardown()
        f2.stop()
