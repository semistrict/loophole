"""Tests for snapshot -- high-level ext4-aware."""

import os
import pytest

from helpers import (
    EXT4_MOUNT,
    FUSE_MOUNT,
    HighLevelMount,
    IS_LINUX,
    LOOPHOLE,
    high_level_format,
    run,
    unique_store_id,
    verify_test_files,
    write_test_files,
)


def test_snapshot_preserves_data():
    parent_id = unique_store_id("snap-parent")
    child_id = unique_store_id("snap-child")
    high_level_format(parent_id)

    m1 = HighLevelMount(parent_id)
    m1.start()
    random_md5 = write_test_files()
    run(LOOPHOLE + ["snapshot", EXT4_MOUNT, "--new-store", child_id])
    m1.stop()

    m2 = HighLevelMount(child_id)
    m2.start()
    try:
        verify_test_files(random_md5)
    finally:
        m2.stop()


def test_child_can_write_independently():
    parent_id = unique_store_id("snap-ind-p")
    child_id = unique_store_id("snap-ind-c")
    high_level_format(parent_id)

    m1 = HighLevelMount(parent_id)
    m1.start()
    with open(f"{EXT4_MOUNT}/parent.txt", "w") as f:
        f.write("from parent\n")
    run("sync")
    run(LOOPHOLE + ["snapshot", EXT4_MOUNT, "--new-store", child_id])
    m1.stop()

    m2 = HighLevelMount(child_id)
    m2.start()
    try:
        with open(f"{EXT4_MOUNT}/child.txt", "w") as f:
            f.write("from child\n")
        run("sync")
        with open(f"{EXT4_MOUNT}/parent.txt") as f:
            assert f.read() == "from parent\n"
        with open(f"{EXT4_MOUNT}/child.txt") as f:
            assert f.read() == "from child\n"
    finally:
        m2.stop()


@pytest.mark.skipif(not IS_LINUX, reason="requires direct FUSE volume access")
def test_writes_fail_on_frozen_parent():
    from helpers import FuseMount

    parent_id = unique_store_id("snap-frozen")
    child_id = unique_store_id("snap-frozen-c")
    high_level_format(parent_id)

    f1 = FuseMount(parent_id)
    f1.start()

    run(LOOPHOLE + ["store", "snapshot", FUSE_MOUNT, "--new-store", child_id])

    try:
        result = os.system(
            f"dd if=/dev/zero of={FUSE_MOUNT}/{parent_id} bs=4k count=1 oflag=direct 2>/dev/null"
        )
        assert result != 0, "write to frozen store should fail"
    finally:
        f1.stop()
