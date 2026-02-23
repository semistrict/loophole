"""Tests for clone -- high-level ext4-aware."""

from helpers import (
    EXT4_MOUNT,
    HighLevelMount,
    LOOPHOLE,
    high_level_format,
    run,
    unique_store_id,
    verify_test_files,
    write_test_files,
)


def test_clone_preserves_data():
    parent_id = unique_store_id("cln-parent")
    clone_id = unique_store_id("cln-clone")
    high_level_format(parent_id)

    m1 = HighLevelMount(parent_id)
    m1.start()
    random_md5 = write_test_files()
    run(LOOPHOLE + ["clone", EXT4_MOUNT, "--clone", clone_id])
    m1.stop()

    m2 = HighLevelMount(clone_id)
    m2.start()
    try:
        verify_test_files(random_md5)
    finally:
        m2.stop()


def test_clone_branches_are_independent():
    parent_id = unique_store_id("cln-ind-p")
    clone_id = unique_store_id("cln-ind-cl")
    high_level_format(parent_id)

    m1 = HighLevelMount(parent_id)
    m1.start()
    with open(f"{EXT4_MOUNT}/shared.txt", "w") as f:
        f.write("from parent\n")
    run("sync")
    run(LOOPHOLE + ["clone", EXT4_MOUNT, "--clone", clone_id])
    m1.stop()

    # Write to clone and verify isolation.
    m2 = HighLevelMount(clone_id)
    m2.start()
    try:
        with open(f"{EXT4_MOUNT}/clone.txt", "w") as f:
            f.write("clone only\n")
        run("sync")
        with open(f"{EXT4_MOUNT}/shared.txt") as f:
            assert f.read() == "from parent\n"
        with open(f"{EXT4_MOUNT}/clone.txt") as f:
            assert f.read() == "clone only\n"
    finally:
        m2.stop()
