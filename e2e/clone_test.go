package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/client"
)

func TestE2E_ClonePreservesData(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "cln-parent")

	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: "cln-parent"}))
	err := b.Mount(ctx, "cln-parent", parentMP)
	require.NoError(t, err)

	tfs := newTestFS(t, b, parentMP)
	randomMD5 := writeTestFiles(t, tfs)

	cloneMP := mountpoint(t, "cln-clone")
	err = b.Clone(t.Context(), parentMP, "cln-clone", cloneMP)
	require.NoError(t, err)

	err = b.Unmount(t.Context(), parentMP)
	require.NoError(t, err)

	verifyTestFiles(t, newTestFS(t, b, cloneMP), randomMD5)
}

func TestE2E_CloneBranchesAreIndependent(t *testing.T) {
	b := newBackend(t)
	ctx := t.Context()
	parentMP := mountpoint(t, "cln-ind-parent")

	require.NoError(t, b.Create(ctx, client.CreateParams{Volume: "cln-ind-parent"}))
	err := b.Mount(ctx, "cln-ind-parent", parentMP)
	require.NoError(t, err)

	parentFS := newTestFS(t, b, parentMP)
	parentFS.WriteFile(t, "shared.txt", []byte("from parent\n"))
	if needsKernelExt4() {
		syncFS(t, parentMP)
	}

	cloneMP := mountpoint(t, "cln-ind-clone")
	err = b.Clone(t.Context(), parentMP, "cln-ind-clone", cloneMP)
	require.NoError(t, err)

	err = b.Unmount(t.Context(), parentMP)
	require.NoError(t, err)

	cloneFS := newTestFS(t, b, cloneMP)
	cloneFS.WriteFile(t, "clone.txt", []byte("clone only\n"))

	require.Equal(t, "from parent\n", string(cloneFS.ReadFile(t, "shared.txt")))
	require.Equal(t, "clone only\n", string(cloneFS.ReadFile(t, "clone.txt")))
}
