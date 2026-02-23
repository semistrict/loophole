"""Filesystem stress tests using fsx and fio."""

import shutil
import pytest
from helpers import EXT4_MOUNT, high_level_format, run, unique_store_id

requires_fsx = pytest.mark.skipif(
    shutil.which("fsx") is None, reason="fsx not installed"
)
requires_fio = pytest.mark.skipif(
    shutil.which("fio") is None, reason="fio not installed"
)


@requires_fsx
def test_fsx_basic(hl_mount):
    """Run fsx with default operations (read, write, truncate, mapread)."""
    store_id = unique_store_id("fsx-basic")
    high_level_format(store_id, block_size="4K", volume_size="64M")

    hl_mount(store_id)
    run(
        ["fsx", "-N", "5000", "-l", "1048576", "-S", "42", f"{EXT4_MOUNT}/fsx-testfile"]
    )


@requires_fsx
def test_fsx_with_punch_hole(hl_mount):
    """Run fsx with fallocate punch hole operations enabled."""
    store_id = unique_store_id("fsx-punch")
    high_level_format(store_id, block_size="4K", volume_size="64M")

    hl_mount(store_id)
    run(
        [
            "fsx",
            "-H",
            "-N",
            "5000",
            "-l",
            "1048576",
            "-S",
            "123",
            f"{EXT4_MOUNT}/fsx-punch-testfile",
        ]
    )


@requires_fsx
def test_fsx_heavy(hl_mount):
    """Longer fsx run with punch hole, zero range, and collapse range."""
    store_id = unique_store_id("fsx-heavy")
    high_level_format(store_id, block_size="4K", volume_size="128M")

    hl_mount(store_id)
    run(
        [
            "fsx",
            "-H",
            "-N",
            "20000",
            "-l",
            "4194304",
            "-S",
            "999",
            f"{EXT4_MOUNT}/fsx-heavy-testfile",
        ]
    )


@requires_fio
def test_fio_random_rw(hl_mount):
    """Random read/write stress test."""
    store_id = unique_store_id("fio-randrw")
    high_level_format(store_id)

    hl_mount(store_id)
    run(
        [
            "fio",
            "--name=randrw",
            f"--directory={EXT4_MOUNT}",
            "--rw=randrw",
            "--bs=4k",
            "--size=8M",
            "--numjobs=2",
            "--runtime=15",
            "--time_based",
            "--verify=crc32c",
            "--verify_fatal=1",
            "--group_reporting",
        ]
    )


@requires_fio
def test_fio_sequential_write_then_verify(hl_mount):
    """Write sequentially, then verify all data."""
    store_id = unique_store_id("fio-seqver")
    high_level_format(store_id)

    hl_mount(store_id)
    run(
        [
            "fio",
            "--name=seqwrite",
            f"--directory={EXT4_MOUNT}",
            "--rw=write",
            "--bs=64k",
            "--size=16M",
            "--verify=sha256",
            "--verify_fatal=1",
            "--do_verify=0",
        ]
    )
    run("sync")
    run(
        [
            "fio",
            "--name=seqwrite",
            f"--directory={EXT4_MOUNT}",
            "--rw=write",
            "--bs=64k",
            "--size=16M",
            "--verify=sha256",
            "--verify_fatal=1",
            "--verify_only",
        ]
    )


@requires_fio
def test_fio_fallocate_punch_hole(hl_mount):
    """Exercise fallocate with punch hole via fio."""
    store_id = unique_store_id("fio-falloc")
    high_level_format(store_id, block_size="4K", volume_size="128M")

    hl_mount(store_id)
    run(
        [
            "fio",
            "--name=falloc",
            f"--directory={EXT4_MOUNT}",
            "--rw=randwrite",
            "--bs=4k",
            "--size=4M",
            "--fallocate=keep",
            "--numjobs=1",
            "--runtime=10",
            "--time_based",
            "--verify=crc32c",
            "--verify_fatal=1",
        ]
    )
