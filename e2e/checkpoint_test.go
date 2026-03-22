package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/internal/storage"
)

// TestE2E_CheckpointPreservesData creates a volume, writes data, checkpoints,
// then clones from the checkpoint and verifies data integrity.
func TestE2E_CheckpointPreservesData(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "cp-pres-parent")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "cp-pres-parent"}))
	require.NoError(t, b.Mount(ctx, "cp-pres-parent", parentMP))

	tfs := newTestFS(t, b, parentMP)
	randomMD5 := writeTestFiles(t, tfs)

	cpID, err := b.Checkpoint(ctx, parentMP)
	require.NoError(t, err)
	require.NotEmpty(t, cpID)

	cloneMP := mountpoint(t, "cp-pres-clone")
	err = b.CloneFromCheckpoint(ctx, "cp-pres-parent", cpID, "cp-pres-clone")
	require.NoError(t, err)
	require.NoError(t, b.Mount(ctx, "cp-pres-clone", cloneMP))

	require.NoError(t, b.Unmount(ctx, parentMP))

	verifyTestFiles(t, newTestFS(t, b, cloneMP), randomMD5)
}

// TestE2E_CheckpointParentStaysWritable verifies the parent volume remains
// writable after a checkpoint.
func TestE2E_CheckpointParentStaysWritable(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "cp-writable")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "cp-writable"}))
	require.NoError(t, b.Mount(ctx, "cp-writable", mp))

	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "before.txt", []byte("before checkpoint\n"))

	_, err := b.Checkpoint(ctx, mp)
	require.NoError(t, err)

	// Parent should still be writable after checkpoint.
	tfs.WriteFile(t, "after.txt", []byte("after checkpoint\n"))

	require.Equal(t, "before checkpoint\n", string(tfs.ReadFile(t, "before.txt")))
	require.Equal(t, "after checkpoint\n", string(tfs.ReadFile(t, "after.txt")))
}

// TestE2E_CheckpointCloneIsIndependent verifies that writes to the clone
// do not affect the parent, and vice versa.
func TestE2E_CheckpointCloneIsIndependent(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "cp-ind-parent")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "cp-ind-parent"}))
	require.NoError(t, b.Mount(ctx, "cp-ind-parent", parentMP))

	parentFS := newTestFS(t, b, parentMP)
	parentFS.WriteFile(t, "shared.txt", []byte("from parent\n"))
	if needsKernelExt4() {
		syncFS(t, parentMP)
	}

	cpID, err := b.Checkpoint(ctx, parentMP)
	require.NoError(t, err)

	// Write to parent after checkpoint.
	parentFS.WriteFile(t, "parent-only.txt", []byte("parent after cp\n"))

	cloneMP := mountpoint(t, "cp-ind-clone")
	err = b.CloneFromCheckpoint(ctx, "cp-ind-parent", cpID, "cp-ind-clone")
	require.NoError(t, err)
	require.NoError(t, b.Mount(ctx, "cp-ind-clone", cloneMP))

	require.NoError(t, b.Unmount(ctx, parentMP))

	cloneFS := newTestFS(t, b, cloneMP)

	// Clone should have the shared file from before checkpoint.
	require.Equal(t, "from parent\n", string(cloneFS.ReadFile(t, "shared.txt")))

	// Clone should NOT have the file written after checkpoint.
	require.False(t, cloneFS.Exists(t, "parent-only.txt"))

	// Write to clone should succeed.
	cloneFS.WriteFile(t, "clone-only.txt", []byte("clone data\n"))
	require.Equal(t, "clone data\n", string(cloneFS.ReadFile(t, "clone-only.txt")))
}

// TestE2E_MultipleCheckpoints verifies that multiple checkpoints can be created
// and each captures the state at its point in time.
func TestE2E_MultipleCheckpoints(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "cp-multi")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "cp-multi"}))
	require.NoError(t, b.Mount(ctx, "cp-multi", mp))

	tfs := newTestFS(t, b, mp)

	// Write file and take first checkpoint.
	tfs.WriteFile(t, "v1.txt", []byte("version 1\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}
	cp1, err := b.Checkpoint(ctx, mp)
	require.NoError(t, err)

	// Write another file and take second checkpoint.
	tfs.WriteFile(t, "v2.txt", []byte("version 2\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}
	cp2, err := b.Checkpoint(ctx, mp)
	require.NoError(t, err)

	require.NotEqual(t, cp1, cp2)

	// List checkpoints — should have both.
	cps, err := b.ListCheckpoints(ctx, "cp-multi")
	require.NoError(t, err)
	require.Len(t, cps, 2)
	require.Equal(t, cp1, cps[0].ID)
	require.Equal(t, cp2, cps[1].ID)

	// Clone from cp1 — should have v1.txt but not v2.txt.
	clone1MP := mountpoint(t, "cp-multi-c1")
	err = b.CloneFromCheckpoint(ctx, "cp-multi", cp1, "cp-multi-c1")
	require.NoError(t, err)
	require.NoError(t, b.Mount(ctx, "cp-multi-c1", clone1MP))

	require.NoError(t, b.Unmount(ctx, mp))

	c1fs := newTestFS(t, b, clone1MP)
	require.Equal(t, "version 1\n", string(c1fs.ReadFile(t, "v1.txt")))
	require.False(t, c1fs.Exists(t, "v2.txt"))

	require.NoError(t, b.Unmount(ctx, clone1MP))

	// Clone from cp2 — should have both files.
	clone2MP := mountpoint(t, "cp-multi-c2")
	err = b.CloneFromCheckpoint(ctx, "cp-multi", cp2, "cp-multi-c2")
	require.NoError(t, err)
	require.NoError(t, b.Mount(ctx, "cp-multi-c2", clone2MP))

	c2fs := newTestFS(t, b, clone2MP)
	require.Equal(t, "version 1\n", string(c2fs.ReadFile(t, "v1.txt")))
	require.Equal(t, "version 2\n", string(c2fs.ReadFile(t, "v2.txt")))
}

// TestE2E_CloneLatestCheckpointAfterUnmount isolates the failing path from
// TestE2E_MultipleCheckpoints: clone the second checkpoint after the parent
// has been unmounted, then verify both files are present in the clone.
func TestE2E_CloneLatestCheckpointAfterUnmount(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "cp-latest")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "cp-latest"}))
	require.NoError(t, b.Mount(ctx, "cp-latest", mp))

	tfs := newTestFS(t, b, mp)

	tfs.WriteFile(t, "v1.txt", []byte("version 1\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}
	_, err := b.Checkpoint(ctx, mp)
	require.NoError(t, err)

	tfs.WriteFile(t, "v2.txt", []byte("version 2\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}
	cp2, err := b.Checkpoint(ctx, mp)
	require.NoError(t, err)

	require.NoError(t, b.Unmount(ctx, mp))

	cloneMP := mountpoint(t, "cp-latest-c2")
	require.NoError(t, b.CloneFromCheckpoint(ctx, "cp-latest", cp2, "cp-latest-c2"))
	require.NoError(t, b.Mount(ctx, "cp-latest-c2", cloneMP))

	cfs := newTestFS(t, b, cloneMP)
	require.Equal(t, "version 1\n", string(cfs.ReadFile(t, "v1.txt")))
	require.Equal(t, "version 2\n", string(cfs.ReadFile(t, "v2.txt")))
}

// TestE2E_CheckpointCloneFromCheckpointMultipleClones verifies that multiple
// clones can be created from the same checkpoint.
func TestE2E_CheckpointMultipleClones(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	mp := mountpoint(t, "cp-mc")

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: "cp-mc"}))
	require.NoError(t, b.Mount(ctx, "cp-mc", mp))

	tfs := newTestFS(t, b, mp)
	tfs.WriteFile(t, "data.txt", []byte("shared data\n"))
	if needsKernelExt4() {
		syncFS(t, mp)
	}

	cpID, err := b.Checkpoint(ctx, mp)
	require.NoError(t, err)

	require.NoError(t, b.Unmount(ctx, mp))

	// Create two clones from the same checkpoint.
	clone1MP := mountpoint(t, "cp-mc-c1")
	err = b.CloneFromCheckpoint(ctx, "cp-mc", cpID, "cp-mc-c1")
	require.NoError(t, err)
	require.NoError(t, b.Mount(ctx, "cp-mc-c1", clone1MP))

	clone2MP := mountpoint(t, "cp-mc-c2")
	err = b.CloneFromCheckpoint(ctx, "cp-mc", cpID, "cp-mc-c2")
	require.NoError(t, err)
	require.NoError(t, b.Mount(ctx, "cp-mc-c2", clone2MP))

	c1fs := newTestFS(t, b, clone1MP)
	c2fs := newTestFS(t, b, clone2MP)

	// Both clones should have the original data.
	require.Equal(t, "shared data\n", string(c1fs.ReadFile(t, "data.txt")))
	require.Equal(t, "shared data\n", string(c2fs.ReadFile(t, "data.txt")))

	// Write different data to each clone.
	c1fs.WriteFile(t, "c1.txt", []byte("clone 1\n"))
	c2fs.WriteFile(t, "c2.txt", []byte("clone 2\n"))

	// Clones are independent.
	require.True(t, c1fs.Exists(t, "c1.txt"))
	require.False(t, c1fs.Exists(t, "c2.txt"))
	require.False(t, c2fs.Exists(t, "c1.txt"))
	require.True(t, c2fs.Exists(t, "c2.txt"))
}
