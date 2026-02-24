import pytest
from helpers import (
    IS_LINUX,
    FuseMount,
    HighLevelMount,
    setup_s3,
)

linux_only = pytest.mark.skipif(
    not IS_LINUX, reason="requires Linux (FUSE + loop devices)"
)


@pytest.fixture(autouse=True, scope="session")
def _s3_ready():
    setup_s3()


@pytest.fixture
def fuse():
    """Factory: call fuse(store_id) to get a started FuseMount.
    All mounts are stopped automatically on teardown."""
    if not IS_LINUX:
        pytest.skip("FuseMount requires Linux")
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
def hl_mount():
    """Factory: call hl_mount(store_id) to get a started HighLevelMount.
    All mounts are stopped automatically on teardown."""
    if not IS_LINUX:
        pytest.skip("HighLevelMount requires Linux (FUSE + loop + kernel ext4)")
    mounts = []

    def _make(store_id, **kwargs):
        m = HighLevelMount(store_id, **kwargs)
        m.start()
        mounts.append(m)
        return m

    yield _make
    for m in reversed(mounts):
        m.stop()
