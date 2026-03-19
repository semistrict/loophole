//go:build linux

package e2e

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/storage"
)

func TestE2E_MetadataChurnDuringFlush(t *testing.T) {
	skipE2E(t)
	skipKernelOnly(t)

	b := newBackend(t)
	ctx := t.Context()
	vol := "meta-churn"
	mp := mountpoint(t, vol)

	require.NoError(t, b.Create(ctx, storage.CreateParams{Volume: vol}))

	// Keep flush aggressive without making the one-off create/format owner
	// path pay the full cost. The mounted owner below is the one this test
	// actually stresses.
	t.Setenv("LOOPHOLE_TEST_STORAGE_FLUSH_THRESHOLD", "16384")
	require.NoError(t, b.Mount(ctx, vol, mp))

	owner := b.ownerByMountpoint(mp)
	require.NotNil(t, owner)

	root := filepath.Join(mp, "usr", "share", "doc")
	require.NoError(t, os.MkdirAll(root, 0o755))

	stop := make(chan struct{})
	var walkers sync.WaitGroup
	errCh := make(chan error, 1)

	startWalker := func(id int) {
		walkers.Add(1)
		go func() {
			defer walkers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						if benignPathRace(err) {
							return nil
						}
						return fmt.Errorf("walker %d walkdir %s: %w", id, path, err)
					}

					info, err := os.Lstat(path)
					if err != nil {
						if benignPathRace(err) {
							return nil
						}
						return fmt.Errorf("walker %d lstat %s: %w", id, path, err)
					}
					if info.IsDir() {
						return nil
					}

					f, err := os.Open(path)
					if err != nil {
						if benignPathRace(err) {
							return nil
						}
						return fmt.Errorf("walker %d open %s: %w", id, path, err)
					}
					defer util.SafeClose(f, "close walker file")

					buf := make([]byte, 128)
					if _, err := io.ReadFull(f, buf); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
						if benignPathRace(err) {
							return nil
						}
						return fmt.Errorf("walker %d read %s: %w", id, path, err)
					}
					return nil
				})
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	const walkersPerRound = 4
	const metadataRounds = 10
	const packagesPerRound = 96

	for i := 0; i < walkersPerRound; i++ {
		startWalker(i)
	}

	for round := 0; round < metadataRounds; round++ {
		for pkg := 0; pkg < packagesPerRound; pkg++ {
			select {
			case err := <-errCh:
				close(stop)
				walkers.Wait()
				t.Fatalf("metadata read failure after round %d pkg %d: %v\nowner log:\n%s", round, pkg, err, tailFile(owner.logPath, 200))
			default:
			}

			dir := filepath.Join(root, fmt.Sprintf("pkg-%03d", pkg))
			require.NoError(t, os.MkdirAll(dir, 0o755))

			payload := bytes.Repeat([]byte{byte(round), byte(pkg), 0x7a, 0x11}, 1024)
			tmp := filepath.Join(dir, fmt.Sprintf(".copyright.tmp.%d", round))
			final := filepath.Join(dir, "copyright")
			require.NoError(t, os.WriteFile(tmp, payload, 0o644))
			require.NoError(t, os.Rename(tmp, final))

			readmeTmp := filepath.Join(dir, fmt.Sprintf(".README.tmp.%d", round))
			readme := filepath.Join(dir, "README.Debian")
			require.NoError(t, os.WriteFile(readmeTmp, append(payload[:2048:2048], '\n'), 0o644))
			require.NoError(t, os.Rename(readmeTmp, readme))

			if pkg%3 == 0 {
				old := filepath.Join(dir, "obsolete")
				require.NoError(t, os.WriteFile(old, payload[:512], 0o644))
				require.NoError(t, os.Remove(old))
			}

			if _, err := os.Lstat(final); err != nil {
				t.Fatalf("writer lstat %s: %v\nowner log:\n%s", final, err, tailFile(owner.logPath, 200))
			}
		}

		syncFS(t, mp)
		time.Sleep(10 * time.Millisecond)
	}

	close(stop)
	walkers.Wait()

	select {
	case err := <-errCh:
		t.Fatalf("metadata read failure: %v\nowner log:\n%s", err, tailFile(owner.logPath, 200))
	default:
	}
}

func benignPathRace(err error) bool {
	return errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, syscall.ENOENT) ||
		errors.Is(err, syscall.ENOTDIR)
}
