package storage2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/semistrict/loophole"
	"github.com/stretchr/testify/require"
)

type overwriteRaceStoreShared struct {
	base    *loophole.MemStore
	target  string
	started chan struct{}
	release chan struct{}
	armed   atomic.Bool
	fired   atomic.Bool
}

type overwriteRaceStore struct {
	shared *overwriteRaceStoreShared
	prefix string
}

type corruptReadStoreShared struct {
	base   *loophole.MemStore
	target string
	armed  atomic.Bool
}

type corruptReadStore struct {
	shared *corruptReadStoreShared
	prefix string
}

func newOverwriteRaceStore(base *loophole.MemStore) *overwriteRaceStore {
	return &overwriteRaceStore{
		shared: &overwriteRaceStoreShared{base: base},
	}
}

func newCorruptReadStore(base *loophole.MemStore) *corruptReadStore {
	return &corruptReadStore{
		shared: &corruptReadStoreShared{base: base},
	}
}

func (s *overwriteRaceStore) arm(fullKey string) {
	s.shared.target = fullKey
	s.shared.started = make(chan struct{})
	s.shared.release = make(chan struct{})
	s.shared.armed.Store(true)
	s.shared.fired.Store(false)
}

func (s *corruptReadStore) arm(fullKey string) {
	s.shared.target = fullKey
	s.shared.armed.Store(true)
}

func (s *overwriteRaceStore) waitForOverwrite(t *testing.T) {
	t.Helper()
	select {
	case <-s.shared.started:
	case <-t.Context().Done():
		t.Fatalf("timed out waiting for overwrite of %s", s.shared.target)
	}
}

func (s *overwriteRaceStore) releaseOverwrite() {
	if s.shared.release != nil {
		close(s.shared.release)
	}
}

func (s *overwriteRaceStore) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *corruptReadStore) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func (s *overwriteRaceStore) At(path string) loophole.ObjectStore {
	prefix := path
	if s.prefix != "" {
		prefix = s.prefix + "/" + path
	}
	return &overwriteRaceStore{shared: s.shared, prefix: prefix}
}

func (s *corruptReadStore) At(path string) loophole.ObjectStore {
	prefix := path
	if s.prefix != "" {
		prefix = s.prefix + "/" + path
	}
	return &corruptReadStore{shared: s.shared, prefix: prefix}
}

func (s *overwriteRaceStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	return s.shared.base.Get(ctx, s.fullKey(key))
}

func (s *corruptReadStore) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	fullKey := s.fullKey(key)
	if s.shared.armed.Load() && fullKey == s.shared.target {
		if data, exists := s.shared.base.GetObject(fullKey); exists {
			truncLen := len(data) / 2
			if truncLen <= 0 {
				truncLen = len(data)
			}
			return io.NopCloser(bytes.NewReader(data[:truncLen])), `"corrupt"`, nil
		}
	}
	return s.shared.base.Get(ctx, fullKey)
}

func (s *overwriteRaceStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	return s.shared.base.GetRange(ctx, s.fullKey(key), offset, length)
}

func (s *corruptReadStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	return s.shared.base.GetRange(ctx, s.fullKey(key), offset, length)
}

func (s *overwriteRaceStore) PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	return s.shared.base.PutBytesCAS(ctx, s.fullKey(key), data, etag)
}

func (s *corruptReadStore) PutBytesCAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	return s.shared.base.PutBytesCAS(ctx, s.fullKey(key), data, etag)
}

func (s *overwriteRaceStore) PutReader(ctx context.Context, key string, r io.Reader) error {
	fullKey := s.fullKey(key)
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if s.shared.armed.Load() && fullKey == s.shared.target {
		if _, exists := s.shared.base.GetObject(fullKey); exists && s.shared.fired.CompareAndSwap(false, true) {
			partialLen := 128
			if partialLen >= len(data) {
				partialLen = len(data) - 1
			}
			if partialLen <= 0 {
				partialLen = len(data) / 2
			}
			partial := data[:partialLen]
			if err := s.shared.base.PutReader(ctx, fullKey, bytes.NewReader(partial)); err != nil {
				return err
			}
			close(s.shared.started)
			<-s.shared.release
		}
	}
	return s.shared.base.PutReader(ctx, fullKey, bytes.NewReader(data))
}

func (s *corruptReadStore) PutReader(ctx context.Context, key string, r io.Reader) error {
	return s.shared.base.PutReader(ctx, s.fullKey(key), r)
}

func (s *overwriteRaceStore) PutIfNotExists(ctx context.Context, key string, data []byte, meta ...map[string]string) error {
	return s.shared.base.PutIfNotExists(ctx, s.fullKey(key), data, meta...)
}

func (s *corruptReadStore) PutIfNotExists(ctx context.Context, key string, data []byte, meta ...map[string]string) error {
	return s.shared.base.PutIfNotExists(ctx, s.fullKey(key), data, meta...)
}

func (s *overwriteRaceStore) DeleteObject(ctx context.Context, key string) error {
	return s.shared.base.DeleteObject(ctx, s.fullKey(key))
}

func (s *corruptReadStore) DeleteObject(ctx context.Context, key string) error {
	return s.shared.base.DeleteObject(ctx, s.fullKey(key))
}

func (s *overwriteRaceStore) ListKeys(ctx context.Context, prefix string) ([]loophole.ObjectInfo, error) {
	return s.shared.base.ListKeys(ctx, s.fullKey(prefix))
}

func (s *corruptReadStore) ListKeys(ctx context.Context, prefix string) ([]loophole.ObjectInfo, error) {
	return s.shared.base.ListKeys(ctx, s.fullKey(prefix))
}

func (s *overwriteRaceStore) HeadMeta(ctx context.Context, key string) (map[string]string, error) {
	return s.shared.base.HeadMeta(ctx, s.fullKey(key))
}

func (s *corruptReadStore) HeadMeta(ctx context.Context, key string) (map[string]string, error) {
	return s.shared.base.HeadMeta(ctx, s.fullKey(key))
}

func (s *overwriteRaceStore) SetMeta(ctx context.Context, key string, meta map[string]string) error {
	return s.shared.base.SetMeta(ctx, s.fullKey(key), meta)
}

func (s *corruptReadStore) SetMeta(ctx context.Context, key string, meta map[string]string) error {
	return s.shared.base.SetMeta(ctx, s.fullKey(key), meta)
}

func TestRepro_SameVolumeReadDuringInPlaceRecompactionCanSeeTornBlock(t *testing.T) {
	ctx := t.Context()
	base := loophole.NewMemStore()
	store := newOverwriteRaceStore(base)

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m.Close(t.Context())
	})

	v, err := m.NewVolume(loophole.CreateParams{
		Volume:   "vol",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())

	vol := v.(*volume)
	require.NoError(t, vol.layer.ForceCompactL0())

	layerID, seq := vol.layer.l1Map.Find(0)
	require.NotEmpty(t, layerID)
	require.Equal(t, vol.layer.id, layerID)
	require.Equal(t, vol.layer.writeLeaseSeq, seq)
	targetKey := blockKey(layerID, "l1", seq, 0)
	store.arm(targetKey)

	// Force the next read of this block to go back to object storage instead of
	// hitting the parsed block cache populated by the first compaction.
	vol.layer.blockCache.clear()

	for i := 20; i < 40; i++ {
		page := bytes.Repeat([]byte{0xBB, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i%BlockPages)*PageSize))
	}
	require.NoError(t, v.Flush())

	var compactWG sync.WaitGroup
	compactWG.Add(1)
	var compactErr error
	go func() {
		defer compactWG.Done()
		compactErr = vol.layer.ForceCompactL0()
	}()

	store.waitForOverwrite(t)
	vol.layer.blockCache.clear()

	buf := make([]byte, PageSize)
	readDone := make(chan error, 1)
	go func() {
		_, readErr := v.Read(ctx, buf, 5*PageSize)
		readDone <- readErr
	}()
	store.releaseOverwrite()
	compactWG.Wait()
	readErr := <-readDone

	require.NoError(t, compactErr)
	require.NoError(t, readErr)
	require.Equal(t, byte(0xAA), buf[0])
	require.Equal(t, byte(5), buf[1])

	t.Logf("auto-healed read while overwriting %s", targetKey)
}

func TestRepro_FrozenLayerReadAutoHealsAcrossCompaction(t *testing.T) {
	ctx := t.Context()
	base := loophole.NewMemStore()
	store := newOverwriteRaceStore(base)

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.(*volume).layer.ForceCompactL0())
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAB, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	frozenVol, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	frozen := frozenVol.(*frozenVolume)

	layerID, seq := frozen.layer.l1Map.Find(0)
	require.NotEmpty(t, layerID)
	targetKey := blockKey(layerID, "l1", seq, 0)
	store.arm(targetKey)

	var compactWG sync.WaitGroup
	compactWG.Add(1)
	var compactErr error
	go func() {
		defer compactWG.Done()
		compactErr = frozen.layer.ForceCompactL0()
	}()

	store.waitForOverwrite(t)
	frozen.layer.blockCache.clear()

	buf := make([]byte, PageSize)
	readDone := make(chan error, 1)
	go func() {
		_, readErr := frozenVol.Read(ctx, buf, 5*PageSize)
		readDone <- readErr
	}()

	store.releaseOverwrite()
	compactWG.Wait()
	readErr := <-readDone

	require.NoError(t, compactErr)
	require.NoError(t, readErr)
	require.Equal(t, byte(0xAB), buf[0])
	require.Equal(t, byte(5), buf[1])
}

func TestRepro_CloneReadAutoHealsAcrossSharedFrozenLayerCompaction(t *testing.T) {
	ctx := t.Context()
	base := loophole.NewMemStore()
	store := newOverwriteRaceStore(base)

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.(*volume).layer.ForceCompactL0())
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAB, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	zygote, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	child := cloneOpen(t, zygote, "child")

	frozen := zygote.(*frozenVolume)
	layerID, seq := frozen.layer.l1Map.Find(0)
	require.NotEmpty(t, layerID)
	targetKey := blockKey(layerID, "l1", seq, 0)
	store.arm(targetKey)

	var compactWG sync.WaitGroup
	compactWG.Add(1)
	var compactErr error
	go func() {
		defer compactWG.Done()
		compactErr = frozen.layer.ForceCompactL0()
	}()

	store.waitForOverwrite(t)
	frozen.layer.blockCache.clear()
	child.(*volume).layer.blockCache.clear()

	buf := make([]byte, PageSize)
	readDone := make(chan error, 1)
	go func() {
		_, readErr := child.Read(ctx, buf, 5*PageSize)
		readDone <- readErr
	}()

	store.releaseOverwrite()
	compactWG.Wait()
	readErr := <-readDone

	require.NoError(t, compactErr)
	require.NoError(t, readErr)
	require.Equal(t, byte(0xAB), buf[0])
	require.Equal(t, byte(5), buf[1])
}

func TestRepro_FrozenReadAtAutoHealsAcrossCompaction(t *testing.T) {
	ctx := t.Context()
	base := loophole.NewMemStore()
	store := newOverwriteRaceStore(base)

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.(*volume).layer.ForceCompactL0())
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAB, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	frozenVol, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	frozen := frozenVol.(*frozenVolume)

	layerID, seq := frozen.layer.l1Map.Find(0)
	require.NotEmpty(t, layerID)
	targetKey := blockKey(layerID, "l1", seq, 0)
	store.arm(targetKey)

	var compactWG sync.WaitGroup
	compactWG.Add(1)
	var compactErr error
	go func() {
		defer compactWG.Done()
		compactErr = frozen.layer.ForceCompactL0()
	}()

	store.waitForOverwrite(t)
	frozen.layer.blockCache.clear()

	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		data, readErr := frozenVol.ReadAt(ctx, 5*PageSize, PageSize)
		readDone <- readResult{data: data, err: readErr}
	}()

	store.releaseOverwrite()
	compactWG.Wait()
	result := <-readDone

	require.NoError(t, compactErr)
	require.NoError(t, result.err)
	require.Equal(t, byte(0xAB), result.data[0])
	require.Equal(t, byte(5), result.data[1])
}

func TestConcurrentFrozenCompactionConverges(t *testing.T) {
	ctx := t.Context()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	store := loophole.NewMemStore()
	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	m3 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m3.Close(t.Context())
	})

	z1, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	z2, err := m3.OpenVolume("zygote")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make(chan error, 2)
	go func() {
		defer wg.Done()
		errs <- z1.(*frozenVolume).layer.ForceCompactL0()
	}()
	go func() {
		defer wg.Done()
		errs <- z2.(*frozenVolume).layer.ForceCompactL0()
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	buf := make([]byte, PageSize)
	_, err = z1.Read(ctx, buf, 5*PageSize)
	require.NoError(t, err)
	require.Equal(t, byte(0xAA), buf[0])
	require.Equal(t, byte(5), buf[1])

	_, _, err = store.At("layers/"+z1.(*frozenVolume).layer.id).Get(ctx, "compaction-lock.json")
	require.Error(t, err)
	require.True(t, errors.Is(err, loophole.ErrNotFound), "compaction lock should be cleaned up, got %v", err)
}

func TestRetryableReadErrorWithoutLayoutChangeReturnsOriginalError(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(loophole.CreateParams{
		Volume:   "vol",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.(*volume).layer.ForceCompactL0())

	vol := v.(*volume)
	layerID, seq := vol.layer.l1Map.Find(0)
	require.NotEmpty(t, layerID)
	targetKey := blockKey(layerID, "l1", seq, 0)
	store.SetFault(loophole.OpGet, "", loophole.Fault{
		Err: fmt.Errorf("bad message"),
		ShouldFault: func(key string) bool {
			return key == targetKey
		},
	})

	vol.layer.blockCache.clear()
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	buf := make([]byte, PageSize)
	_, err = v.Read(readCtx, buf, 5*PageSize)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad message")
	require.Equal(t, uint64(2), vol.layer.index.LayoutGen)
}

func TestFrozenCompactionPersistsLayoutGeneration(t *testing.T) {
	ctx := t.Context()
	store := loophole.NewMemStore()
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m1 := newTestManager(t, store, cfg)
	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.(*volume).layer.ForceCompactL0())
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAB, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	m2 := newTestManager(t, store, cfg)
	frozenVol, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	frozen := frozenVol.(*frozenVolume)
	prevGen := frozen.layer.index.LayoutGen

	require.NoError(t, frozen.layer.ForceCompactL0())
	require.Greater(t, frozen.layer.index.LayoutGen, prevGen)

	idx, _, err := loophole.ReadJSON[layerIndex](ctx, store.At("layers/"+frozen.layer.id), "index.json")
	require.NoError(t, err)
	require.Equal(t, frozen.layer.index.LayoutGen, idx.LayoutGen)
}

func TestFollowerLayerReadAutoHealsAcrossSharedCompaction(t *testing.T) {
	ctx := t.Context()
	base := loophole.NewMemStore()
	store := newOverwriteRaceStore(base)

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.(*volume).layer.ForceCompactL0())
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAB, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.Freeze())

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	frozenVol, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	frozen := frozenVol.(*frozenVolume)

	follower, err := openLayer(ctx, store, frozen.layer.id, cfg, nil, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		follower.Close()
	})

	layerID, seq := frozen.layer.l1Map.Find(0)
	require.NotEmpty(t, layerID)
	targetKey := blockKey(layerID, "l1", seq, 0)
	store.arm(targetKey)

	var compactWG sync.WaitGroup
	compactWG.Add(1)
	var compactErr error
	go func() {
		defer compactWG.Done()
		compactErr = frozen.layer.ForceCompactL0()
	}()

	store.waitForOverwrite(t)
	follower.blockCache.clear()

	buf := make([]byte, PageSize)
	readDone := make(chan error, 1)
	go func() {
		_, readErr := follower.Read(ctx, buf, 5*PageSize)
		readDone <- readErr
	}()

	store.releaseOverwrite()
	compactWG.Wait()
	readErr := <-readDone

	require.NoError(t, compactErr)
	require.NoError(t, readErr)
	require.Equal(t, byte(0xAB), buf[0])
	require.Equal(t, byte(5), buf[1])
}

func TestLayoutGenerationPersistsAcrossCompactionAndReopen(t *testing.T) {
	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	store := loophole.NewMemStore()
	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "vol",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())

	vol := v.(*volume)
	before := vol.layer.index.LayoutGen
	require.NoError(t, vol.layer.ForceCompactL0())
	after := vol.layer.index.LayoutGen
	require.Equal(t, before+1, after)
	require.NoError(t, m1.Close(t.Context()))

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	reopened, err := m2.OpenVolume("vol")
	require.NoError(t, err)
	require.Equal(t, after, reopened.(*volume).layer.index.LayoutGen)

	require.NoError(t, reopened.Flush())
	require.NoError(t, reopened.(*volume).layer.ForceCompactL0())
	require.Equal(t, after, reopened.(*volume).layer.index.LayoutGen)
}

func TestReadReturnsErrorWhenLayoutDidNotChange(t *testing.T) {
	ctx := t.Context()
	base := loophole.NewMemStore()
	store := newCorruptReadStore(base)

	cfg := Config{
		FlushThreshold:  16 * PageSize,
		MaxFrozenTables: 2,
		FlushInterval:   -1,
		L0PagesMax:      10,
	}

	m1 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m1.Close(t.Context())
	})

	v, err := m1.NewVolume(loophole.CreateParams{
		Volume:   "zygote",
		Size:     uint64(BlockPages) * PageSize,
		NoFormat: true,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		page := bytes.Repeat([]byte{0xAA, byte(i)}, PageSize/2)
		require.NoError(t, v.Write(page, uint64(i)*PageSize))
	}
	require.NoError(t, v.Flush())
	require.NoError(t, v.(*volume).layer.ForceCompactL0())
	require.NoError(t, v.Freeze())

	m2 := NewVolumeManager(store, t.TempDir(), cfg, nil, nil)
	t.Cleanup(func() {
		_ = m2.Close(t.Context())
	})
	frozenVol, err := m2.OpenVolume("zygote")
	require.NoError(t, err)
	frozen := frozenVol.(*frozenVolume)

	layerID, seq := frozen.layer.l1Map.Find(0)
	require.NotEmpty(t, layerID)
	store.arm(blockKey(layerID, "l1", seq, 0))

	buf := make([]byte, PageSize)
	_, err = frozen.layer.Read(ctx, buf, 5*PageSize)
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse block")
}
