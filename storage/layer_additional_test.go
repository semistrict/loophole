package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/semistrict/loophole/objstore"
	"github.com/stretchr/testify/require"
)

func TestLayerStopPeriodicFlush(t *testing.T) {
	ctx := t.Context()
	ly, err := openLayer(ctx, layerParams{
		store:  objstore.NewMemStore(),
		id:     "periodic-flush",
		config: Config{FlushThreshold: 4 * PageSize, FlushInterval: 10 * time.Millisecond},
	})
	require.NoError(t, err)
	defer ly.Close()

	ly.startPeriodicFlush(context.Background())
	require.NotNil(t, ly.flushStop)
	require.NotNil(t, ly.flushDone)
	require.NotNil(t, ly.flushNotify)
	require.NotNil(t, ly.writeNotify)

	ly.stopPeriodicFlush()
	require.Nil(t, ly.flushStop)
	require.Nil(t, ly.flushDone)
	require.Nil(t, ly.flushNotify)
	require.Nil(t, ly.writeNotify)

	ly.stopPeriodicFlush()
}

func TestLayerWaitRefreshForLayoutChange(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	ly, err := openLayer(ctx, layerParams{
		store:  store,
		id:     "refresh-layer",
		config: Config{FlushThreshold: 4 * PageSize, FlushInterval: -1},
	})
	require.NoError(t, err)
	defer ly.Close()
	require.NoError(t, ly.saveIndex(ctx))

	go func() {
		time.Sleep(20 * time.Millisecond)
		err := objstore.ModifyJSON[layerIndex](ctx, store.At("layers/"+ly.id), "index.json", func(idx *layerIndex) error {
			idx.LayoutGen++
			return nil
		})
		require.NoError(t, err)
	}()

	changed, err := ly.waitRefreshForLayoutChange(ctx, ly.index.LayoutGen)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, uint64(2), ly.index.LayoutGen)
}

type fakePageCache struct {
	pages map[string][]byte
}

func (f *fakePageCache) Close() error { return nil }

func (f *fakePageCache) GetPage(layerID string, pageIdx uint64) []byte {
	if data, ok := f.pages[layerCacheKey(layerID, pageIdx)]; ok {
		return append([]byte(nil), data...)
	}
	return nil
}

func (f *fakePageCache) GetPageRef(_ safepoint.Guard, layerID string, pageIdx uint64) []byte {
	return f.pages[layerCacheKey(layerID, pageIdx)]
}

func (f *fakePageCache) PutPage(layerID string, pageIdx uint64, data []byte) {
	f.pages[layerCacheKey(layerID, pageIdx)] = append([]byte(nil), data...)
}

func layerCacheKey(layerID string, pageIdx uint64) string {
	return fmt.Sprintf("%s:%d", layerID, pageIdx)
}

func TestLayerReadPageRefUsesPersistentCache(t *testing.T) {
	ctx := t.Context()
	sp := safepoint.New()
	ly, err := openLayer(ctx, layerParams{
		store:     objstore.NewMemStore(),
		id:        "child",
		config:    Config{FlushThreshold: 4 * PageSize, FlushInterval: -1},
		safepoint: sp,
	})
	require.NoError(t, err)
	defer ly.Close()

	cached := bytes.Repeat([]byte{0xAB}, PageSize)
	ly.diskCache = &fakePageCache{
		pages: map[string][]byte{layerCacheKey("parent", 0): cached},
	}

	snap := &layerSnapshot{
		layoutGen: ly.index.LayoutGen,
		active:    ly.active,
		l1: newBlockRangeMap([]blockRange{{
			Start:         0,
			End:           1,
			Layer:         "parent",
			WriteLeaseSeq: 1,
		}}),
		l2: newBlockRangeMap(nil),
	}

	g := sp.Enter()
	got, err := ly.readPageRef(ctx, g, snap, 0)
	g.Exit()
	require.NoError(t, err)
	require.Equal(t, cached, got)
}

func TestLayerShouldRetryAfterLayoutError(t *testing.T) {
	ly := &layer{}
	require.False(t, ly.shouldRetryAfterLayoutError(nil))
	require.True(t, ly.shouldRetryAfterLayoutError(objstore.ErrNotFound))
	require.True(t, ly.shouldRetryAfterLayoutError(errors.New("parse block failed")))
	require.True(t, ly.shouldRetryAfterLayoutError(errors.New("bad magic in l0 block")))
	require.True(t, ly.shouldRetryAfterLayoutError(errors.New("crc mismatch")))
	require.False(t, ly.shouldRetryAfterLayoutError(errors.New("permission denied")))
}

func TestLayerPunchHolePartialPages(t *testing.T) {
	ctx := t.Context()
	ly, err := openLayer(ctx, layerParams{
		store:  objstore.NewMemStore(),
		id:     "partial-punch",
		config: Config{FlushThreshold: 4 * PageSize, FlushInterval: -1},
	})
	require.NoError(t, err)
	defer ly.Close()

	page0 := bytes.Repeat([]byte{0xAA}, PageSize)
	page1 := bytes.Repeat([]byte{0xBB}, PageSize)
	require.NoError(t, ly.Write(page0, 0))
	require.NoError(t, ly.Write(page1, PageSize))

	require.NoError(t, ly.PunchHole(100, PageSize))

	buf := make([]byte, 2*PageSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, 2*PageSize, n)

	require.Equal(t, bytes.Repeat([]byte{0xAA}, 100), buf[:100])
	require.Equal(t, make([]byte, PageSize-100), buf[100:PageSize])
	require.Equal(t, make([]byte, 100), buf[PageSize:PageSize+100])
	require.Equal(t, bytes.Repeat([]byte{0xBB}, PageSize-100), buf[PageSize+100:])
}

func TestLayerReadRetriesAfterLayoutRefresh(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	ly, err := openLayerReadOnly(ctx, layerParams{
		store:  store,
		id:     "retry-read",
		config: Config{FlushThreshold: 4 * PageSize, FlushInterval: -1},
	})
	require.NoError(t, err)
	defer ly.Close()

	ly.index = layerIndex{
		NextSeq:   1,
		LayoutGen: 1,
		L1: []blockRange{{
			Start:         0,
			End:           1,
			Layer:         "missing",
			WriteLeaseSeq: 1,
		}},
	}
	ly.l1Map = newBlockRangeMap(ly.index.L1)
	ly.l2Map = newBlockRangeMap(nil)
	require.NoError(t, ly.saveIndex(ctx))

	go func() {
		time.Sleep(20 * time.Millisecond)
		err := objstore.ModifyJSON[layerIndex](ctx, store.At("layers/"+ly.id), "index.json", func(idx *layerIndex) error {
			idx.LayoutGen++
			idx.L1 = nil
			idx.L2 = nil
			return nil
		})
		require.NoError(t, err)
	}()

	buf := make([]byte, PageSize)
	n, err := ly.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, PageSize, n)
	require.Equal(t, zeroPage[:], buf)
}

func TestLayerReadPagesRetriesAfterLayoutRefresh(t *testing.T) {
	ctx := t.Context()
	store := objstore.NewMemStore()
	sp := safepoint.New()
	ly, err := openLayerReadOnly(ctx, layerParams{
		store:     store,
		id:        "retry-readpages",
		config:    Config{FlushThreshold: 4 * PageSize, FlushInterval: -1},
		safepoint: sp,
	})
	require.NoError(t, err)
	defer ly.Close()

	ly.index = layerIndex{
		NextSeq:   1,
		LayoutGen: 1,
		L1: []blockRange{{
			Start:         0,
			End:           1,
			Layer:         "missing",
			WriteLeaseSeq: 1,
		}},
	}
	ly.l1Map = newBlockRangeMap(ly.index.L1)
	ly.l2Map = newBlockRangeMap(nil)
	require.NoError(t, ly.saveIndex(ctx))

	go func() {
		time.Sleep(20 * time.Millisecond)
		err := objstore.ModifyJSON[layerIndex](ctx, store.At("layers/"+ly.id), "index.json", func(idx *layerIndex) error {
			idx.LayoutGen++
			idx.L1 = nil
			idx.L2 = nil
			return nil
		})
		require.NoError(t, err)
	}()

	g := sp.Enter()
	slices, err := ly.ReadPages(ctx, g, 0, PageSize)
	g.Exit()
	require.NoError(t, err)
	require.Len(t, slices, 1)
	require.Equal(t, zeroPage[:], slices[0])
}

func TestLayerWaitRefreshForLayoutChangeTimeoutAndCancel(t *testing.T) {
	ctx := t.Context()
	ly, err := openLayer(ctx, layerParams{
		store:  objstore.NewMemStore(),
		id:     "wait-refresh",
		config: Config{FlushThreshold: 4 * PageSize, FlushInterval: -1},
	})
	require.NoError(t, err)
	defer ly.Close()
	require.NoError(t, ly.saveIndex(ctx))

	changed, err := ly.waitRefreshForLayoutChange(ctx, ly.index.LayoutGen)
	require.NoError(t, err)
	require.False(t, changed)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	changed, err = ly.waitRefreshForLayoutChange(cancelCtx, ly.index.LayoutGen)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, changed)
}
