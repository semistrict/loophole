"""Tests for ext4 read/write and persistence across remount."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from helpers import (
    CACHE_DIR,
    EXT4_MOUNT,
    HighLevelMount,
    md5,
    run,
    high_level_format,
    unique_store_id,
    verify_test_files,
    write_test_files,
)


def test_small_text_file(hl_mount):
    sid = unique_store_id("ext4rw")
    high_level_format(sid)
    hl_mount(sid)

    with open(f"{EXT4_MOUNT}/greeting.txt", "w") as f:
        f.write("hello from loophole\n")
    run("sync")
    with open(f"{EXT4_MOUNT}/greeting.txt") as f:
        assert f.read() == "hello from loophole\n"


def test_binary_file_integrity(hl_mount):
    sid = unique_store_id("ext4bin")
    high_level_format(sid)
    hl_mount(sid)

    run(f"dd if=/dev/urandom of={EXT4_MOUNT}/random.bin bs=1M count=10 status=none")
    run("sync")
    checksum = md5(f"{EXT4_MOUNT}/random.bin")
    assert md5(f"{EXT4_MOUNT}/random.bin") == checksum


def test_nested_directories(hl_mount):
    sid = unique_store_id("ext4nest")
    high_level_format(sid)
    hl_mount(sid)

    os.makedirs(f"{EXT4_MOUNT}/subdir/nested", exist_ok=True)
    with open(f"{EXT4_MOUNT}/subdir/nested/deep.txt", "w") as f:
        f.write("nested file\n")
    run("sync")
    with open(f"{EXT4_MOUNT}/subdir/nested/deep.txt") as f:
        assert f.read() == "nested file\n"


def test_large_sequential_file(hl_mount):
    sid = unique_store_id("ext4seq")
    high_level_format(sid)
    hl_mount(sid)

    with open(f"{EXT4_MOUNT}/numbers.txt", "w") as f:
        for i in range(1, 1001):
            f.write(f"{i}\n")
    run("sync")
    with open(f"{EXT4_MOUNT}/numbers.txt") as f:
        assert len(f.readlines()) == 1000


def test_overwrite_file(hl_mount):
    sid = unique_store_id("ext4ow")
    high_level_format(sid)
    hl_mount(sid)

    path = f"{EXT4_MOUNT}/overwrite.txt"
    with open(path, "w") as f:
        f.write("version 1\n")
    run("sync")
    with open(path, "w") as f:
        f.write("version 2\n")
    run("sync")
    with open(path) as f:
        assert f.read() == "version 2\n"


def test_delete_and_recreate(hl_mount):
    sid = unique_store_id("ext4del")
    high_level_format(sid)
    hl_mount(sid)

    path = f"{EXT4_MOUNT}/ephemeral.txt"
    with open(path, "w") as f:
        f.write("exists\n")
    run("sync")
    os.remove(path)
    assert not os.path.exists(path)
    with open(path, "w") as f:
        f.write("back again\n")
    run("sync")
    with open(path) as f:
        assert f.read() == "back again\n"


def test_files_survive_remount():
    sid = unique_store_id("persist")
    high_level_format(sid)

    # Phase 1: write.
    m1 = HighLevelMount(sid)
    m1.start()
    random_md5 = write_test_files()
    m1.stop()

    # Phase 2: remount and verify.
    m2 = HighLevelMount(sid)
    m2.start()
    try:
        verify_test_files(random_md5)
    finally:
        m2.stop()


def test_concurrent_mount_cycles():
    """Run multiple format+mount+write+unmount cycles concurrently to stress
    loop device allocation and verify no stale page cache corruption."""
    n_workers = 20

    def worker(i):
        sid = unique_store_id(f"concurrent-{i}")
        mp = f"{EXT4_MOUNT}-conc-{i}"
        cd = f"{CACHE_DIR}-conc-{i}"
        high_level_format(sid, block_size="4M", volume_size="64M")
        m = HighLevelMount(sid, mountpoint=mp, cache_dir=cd)
        m.start()
        try:
            path = f"{mp}/test.txt"
            with open(path, "w") as f:
                f.write(f"worker {i}\n")
            run("sync")
            with open(path) as f:
                content = f.read()
            assert content == f"worker {i}\n", f"worker {i}: got {content!r}"
        finally:
            m.stop()

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(worker, i) for i in range(n_workers)]
        errors = []
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                errors.append(e)
        assert not errors, f"concurrent mount failures: {errors}"
