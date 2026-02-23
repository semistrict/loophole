"""Tests for high-level format and mount commands."""

import os

from helpers import EXT4_MOUNT, LOOPHOLE, md5, run, unique_store_id


def test_format_creates_mountable_ext4(hl_mount):
    sid = unique_store_id("hlfmt")
    run(LOOPHOLE + ["format", "--store", sid, "--volume-size", "1G"])

    hl_mount(sid)
    with open(f"{EXT4_MOUNT}/hello.txt", "w") as f:
        f.write("formatted via high-level\n")
    run("sync")
    with open(f"{EXT4_MOUNT}/hello.txt") as f:
        assert f.read() == "formatted via high-level\n"


def test_format_custom_sizes(hl_mount):
    sid = unique_store_id("hlfmt-sz")
    run(
        LOOPHOLE
        + ["format", "--store", sid, "--block-size", "1M", "--volume-size", "64M"]
    )

    hl_mount(sid)
    with open(f"{EXT4_MOUNT}/test.txt", "w") as f:
        f.write("small volume\n")
    run("sync")
    with open(f"{EXT4_MOUNT}/test.txt") as f:
        assert f.read() == "small volume\n"


def test_data_persists_across_mount_cycles(hl_mount):
    sid = unique_store_id("hlmnt")
    run(LOOPHOLE + ["format", "--store", sid, "--volume-size", "1G"])

    from helpers import HighLevelMount as HLM

    # Mount, write, unmount.
    m1 = HLM(sid)
    m1.start()
    run(f"dd if=/dev/urandom of={EXT4_MOUNT}/random.bin bs=1M count=5 status=none")
    run("sync")
    checksum = md5(f"{EXT4_MOUNT}/random.bin")
    m1.stop()

    # Remount and verify.
    m2 = HLM(sid)
    m2.start()
    try:
        assert md5(f"{EXT4_MOUNT}/random.bin") == checksum
    finally:
        m2.stop()


def test_nested_dirs_and_large_file(hl_mount):
    sid = unique_store_id("hlmnt-nd")
    run(LOOPHOLE + ["format", "--store", sid, "--volume-size", "1G"])

    hl_mount(sid)
    os.makedirs(f"{EXT4_MOUNT}/a/b/c", exist_ok=True)
    with open(f"{EXT4_MOUNT}/a/b/c/deep.txt", "w") as f:
        f.write("deep nested\n")
    run(f"dd if=/dev/urandom of={EXT4_MOUNT}/a/big.bin bs=1M count=20 status=none")
    run("sync")

    with open(f"{EXT4_MOUNT}/a/b/c/deep.txt") as f:
        assert f.read() == "deep nested\n"
    assert os.path.getsize(f"{EXT4_MOUNT}/a/big.bin") == 20 * 1024 * 1024
