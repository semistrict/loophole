import pytest
from helpers import (
    FUSE_MOUNT, FuseMount, HighLevelMount, LoopExt4, setup_s3,
)


@pytest.fixture(autouse=True, scope="session")
def _s3_ready():
    setup_s3()


@pytest.fixture
def fuse():
    """Factory: call fuse(store_id) to get a started FuseMount.
    All mounts are stopped automatically on teardown."""
    mounts = []

    def _make(store_id, **kwargs):
        m = FuseMount(store_id, **kwargs)
        m.start()
        mounts.append(m)
        return m

    yield _make
    for m in reversed(mounts):
        m.stop()


@pytest.fixture
def ext4():
    """Factory: call ext4(volume_path) to get a mounted LoopExt4.
    All loop mounts are torn down automatically."""
    loops = []

    def _make(volume_path, format_fs=True, **kwargs):
        e = LoopExt4(volume_path, **kwargs)
        e.setup(format_fs=format_fs)
        loops.append(e)
        return e

    yield _make
    for e in reversed(loops):
        e.teardown()


@pytest.fixture
def hl_mount():
    """Factory: call hl_mount(store_id) to get a started HighLevelMount.
    All mounts are stopped automatically on teardown."""
    mounts = []

    def _make(store_id, **kwargs):
        m = HighLevelMount(store_id, **kwargs)
        m.start()
        mounts.append(m)
        return m

    yield _make
    for m in reversed(mounts):
        m.stop()
