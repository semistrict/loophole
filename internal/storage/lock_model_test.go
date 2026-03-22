package storage

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/semistrict/loophole/internal/blob"
	"github.com/semistrict/loophole/internal/safepoint"
	"github.com/stretchr/testify/require"
)

func fillBlock(b byte) []byte {
	return bytes.Repeat([]byte{b}, BlockSize)
}

func copyReadPagesResult(slices [][]byte) []byte {
	total := 0
	for _, s := range slices {
		total += len(s)
	}
	out := make([]byte, 0, total)
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}

func putTestBlock(t *testing.T, store *blob.Store, cfg Config, layerID, level string, seq uint64, blockIdx BlockIdx, data []byte) {
	t.Helper()
	require.Len(t, data, BlockSize)
	cfg.setDefaults()

	pages := make([]blockPage, BlockPages)
	for i := range BlockPages {
		pages[i] = blockPage{
			offset: uint16(i),
			data:   data[i*PageSize : (i+1)*PageSize],
		}
	}

	blob, err := buildBlock(blockIdx, pages, !cfg.DisableCompression)
	require.NoError(t, err)
	require.NoError(t, store.PutReader(context.Background(), blockKey(layerID, level, seq, blockIdx), bytes.NewReader(blob)))
}

func TestLayerReadBlocksFlushCommit(t *testing.T) {
	ctx := t.Context()
	mem := blob.NewMemDriver()
	store := blob.New(mem)
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "read-blocks-flush-commit", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	oldBlock := fillBlock(0x11)
	require.NoError(t, ly.Write(oldBlock, 0))

	ly.mu.RLock()
	_, seq := ly.l2Map.Find(0)
	ly.mu.RUnlock()
	oldKey := blockKey(ly.id, "l2", seq, 0)
	ly.blockCache.clear()

	readStarted := make(chan struct{})
	unblockRead := make(chan struct{})
	var getCalls atomic.Int32
	mem.SetFault(blob.OpGet, oldKey, blob.Fault{
		Hook: func() {
			if getCalls.Add(1) != 1 {
				return
			}
			close(readStarted)
			<-unblockRead
		},
	})

	type readResult struct {
		buf []byte
		err error
	}
	readDone := make(chan readResult, 1)
	go func() {
		buf := make([]byte, BlockSize)
		_, err := ly.Read(ctx, buf, 0)
		readDone <- readResult{buf: buf, err: err}
	}()

	<-readStarted

	page0 := bytes.Repeat([]byte{0x22}, PageSize)
	writeStarted := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		close(writeStarted)
		if err := ly.Write(page0, 0); err != nil {
			writeDone <- err
			return
		}
		writeDone <- ly.Flush()
	}()

	<-writeStarted
	select {
	case err := <-writeDone:
		t.Fatalf("flush finished before read released: %v", err)
	default:
	}

	close(unblockRead)
	readRes := <-readDone
	require.NoError(t, readRes.err)
	require.Equal(t, oldBlock, readRes.buf)
	require.NoError(t, <-writeDone)

	final := make([]byte, BlockSize)
	_, err = ly.Read(ctx, final, 0)
	require.NoError(t, err)
	want := append([]byte(nil), oldBlock...)
	copy(want[:PageSize], page0)
	require.Equal(t, want, final)
}

func TestLayerReadBlocksDirectL2Commit(t *testing.T) {
	ctx := t.Context()
	mem := blob.NewMemDriver()
	store := blob.New(mem)
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}

	ly, err := openLayer(ctx, layerParams{store: store, id: "read-blocks-directl2-commit", config: cfg})
	require.NoError(t, err)
	defer ly.Close()

	oldBlock := fillBlock(0x31)
	require.NoError(t, ly.Write(oldBlock, 0))

	ly.mu.RLock()
	_, seq := ly.l2Map.Find(0)
	ly.mu.RUnlock()
	oldKey := blockKey(ly.id, "l2", seq, 0)
	ly.blockCache.clear()

	readStarted := make(chan struct{})
	unblockRead := make(chan struct{})
	var getCalls atomic.Int32
	mem.SetFault(blob.OpGet, oldKey, blob.Fault{
		Hook: func() {
			if getCalls.Add(1) != 1 {
				return
			}
			close(readStarted)
			<-unblockRead
		},
	})

	type readResult struct {
		buf []byte
		err error
	}
	readDone := make(chan readResult, 1)
	go func() {
		buf := make([]byte, BlockSize)
		_, err := ly.Read(ctx, buf, 0)
		readDone <- readResult{buf: buf, err: err}
	}()

	<-readStarted

	newBlock := fillBlock(0x41)
	writeStarted := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		close(writeStarted)
		writeDone <- ly.Write(newBlock, 0)
	}()

	<-writeStarted
	select {
	case err := <-writeDone:
		t.Fatalf("direct-L2 write finished before read released: %v", err)
	default:
	}

	close(unblockRead)
	readRes := <-readDone
	require.NoError(t, readRes.err)
	require.Equal(t, oldBlock, readRes.buf)
	require.NoError(t, <-writeDone)

	final := make([]byte, BlockSize)
	_, err = ly.Read(ctx, final, 0)
	require.NoError(t, err)
	require.Equal(t, newBlock, final)
}

func TestLayerReadPagesFlushCommit(t *testing.T) {
	ctx := t.Context()
	mem := blob.NewMemDriver()
	store := blob.New(mem)
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}
	sp := safepoint.New()

	ly, err := openLayer(ctx, layerParams{
		store:     store,
		id:        "readpages-flush-commit",
		config:    cfg,
		safepoint: sp,
	})
	require.NoError(t, err)
	defer ly.Close()

	oldBlock := fillBlock(0x51)
	require.NoError(t, ly.Write(oldBlock, 0))

	ly.mu.RLock()
	_, seq := ly.l2Map.Find(0)
	ly.mu.RUnlock()
	oldKey := blockKey(ly.id, "l2", seq, 0)
	ly.blockCache.clear()

	readStarted := make(chan struct{})
	unblockRead := make(chan struct{})
	var getCalls atomic.Int32
	mem.SetFault(blob.OpGet, oldKey, blob.Fault{
		Hook: func() {
			if getCalls.Add(1) != 1 {
				return
			}
			close(readStarted)
			<-unblockRead
		},
	})

	type readResult struct {
		buf []byte
		err error
	}
	readDone := make(chan readResult, 1)
	go func() {
		g := sp.Enter()
		slices, err := ly.ReadPages(ctx, g, 0, BlockSize)
		buf := copyReadPagesResult(slices)
		g.Exit()
		readDone <- readResult{buf: buf, err: err}
	}()

	<-readStarted

	page0 := bytes.Repeat([]byte{0x52}, PageSize)
	writeStarted := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		close(writeStarted)
		if err := ly.Write(page0, 0); err != nil {
			writeDone <- err
			return
		}
		writeDone <- ly.Flush()
	}()

	<-writeStarted
	select {
	case err := <-writeDone:
		t.Fatalf("flush finished before ReadPages released: %v", err)
	default:
	}

	close(unblockRead)
	readRes := <-readDone
	require.NoError(t, readRes.err)
	require.Equal(t, oldBlock, readRes.buf)
	require.NoError(t, <-writeDone)

	final := make([]byte, BlockSize)
	_, err = ly.Read(ctx, final, 0)
	require.NoError(t, err)
	want := append([]byte(nil), oldBlock...)
	copy(want[:PageSize], page0)
	require.Equal(t, want, final)
}

func TestLayerFollowerMidReadRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		mem := blob.NewMemDriver()
		store := blob.New(mem)
		cfg := Config{
			FlushThreshold: 4 * PageSize,
			FlushInterval:  -1,
		}

		ly, err := openLayerReadOnly(ctx, layerParams{
			store:  store,
			id:     "follower-mid-read-refresh",
			config: cfg,
		})
		require.NoError(t, err)
		defer ly.Close()

		oldBlock0 := fillBlock(0x61)
		newBlock0 := fillBlock(0x71)
		newBlock1 := fillBlock(0x72)

		putTestBlock(t, store, cfg, ly.id, "l1", 1, 0, oldBlock0)
		putTestBlock(t, store, cfg, ly.id, "l1", 2, 0, newBlock0)
		putTestBlock(t, store, cfg, ly.id, "l1", 2, 1, newBlock1)

		ly.index = layerIndex{
			NextSeq:   3,
			LayoutGen: 1,
			L1: []blockRange{
				{Start: 0, End: 1, Layer: ly.id, WriteLeaseSeq: 1},
				{Start: 1, End: 2, Layer: "missing", WriteLeaseSeq: 1},
			},
		}
		ly.l1Map = newBlockRangeMap(ly.index.L1)
		ly.l2Map = newBlockRangeMap(nil)
		require.NoError(t, ly.saveIndex(ctx))

		oldKey := blockKey(ly.id, "l1", 1, 0)
		readStarted := make(chan struct{})
		unblockRead := make(chan struct{})
		var getCalls atomic.Int32
		mem.SetFault(blob.OpGet, oldKey, blob.Fault{
			Hook: func() {
				if getCalls.Add(1) != 1 {
					return
				}
				close(readStarted)
				<-unblockRead
			},
		})

		type readResult struct {
			buf []byte
			err error
		}
		readDone := make(chan readResult, 1)
		go func() {
			buf := make([]byte, 2*BlockSize)
			_, err := ly.Read(ctx, buf, 0)
			readDone <- readResult{buf: buf, err: err}
		}()

		<-readStarted
		err = blob.ModifyJSON[layerIndex](ctx, store.At("layers/"+ly.id), "index.json", func(idx *layerIndex) error {
			idx.LayoutGen++
			idx.L1 = []blockRange{
				{Start: 0, End: 1, Layer: ly.id, WriteLeaseSeq: 2},
				{Start: 1, End: 2, Layer: ly.id, WriteLeaseSeq: 2},
			}
			idx.L2 = nil
			return nil
		})
		require.NoError(t, err)
		close(unblockRead)

		readRes := <-readDone
		require.NoError(t, readRes.err)
		require.Equal(t, append(newBlock0, newBlock1...), readRes.buf)
	})
}

func TestVolumeCloseFlushesAcceptedWritesBeforeReturn(t *testing.T) {
	ctx := t.Context()
	store := blob.New(blob.NewMemDriver())
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}

	m := newTestManager(t, store, cfg)
	v, err := m.NewVolume(CreateParams{Volume: "close-flushes-accepted", Size: 4 * BlockSize})
	require.NoError(t, err)

	page0 := bytes.Repeat([]byte{0x81}, PageSize)
	page1 := bytes.Repeat([]byte{0x82}, PageSize)
	require.NoError(t, v.Write(page0, 0))
	require.NoError(t, v.Write(page1, PageSize))
	require.NoError(t, v.Close())

	m2 := &Manager{BlobStore: store, config: cfg}
	t.Cleanup(func() { _ = m2.Close() })

	reopened, err := m2.OpenVolume("close-flushes-accepted")
	require.NoError(t, err)

	buf := make([]byte, PageSize)
	_, err = reopened.Read(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, page0, buf)
	_, err = reopened.Read(ctx, buf, PageSize)
	require.NoError(t, err)
	require.Equal(t, page1, buf)
}

func TestLayerPunchHoleVsFullBlockWriteSameBlock(t *testing.T) {
	ctx := t.Context()
	cfg := Config{
		FlushThreshold: 8 * PageSize,
		FlushInterval:  -1,
	}

	blockData := fillBlock(0x91)
	punchLast := append(make([]byte, PageSize), blockData[PageSize:]...)

	for i := 0; i < 64; i++ {
		store := blob.New(blob.NewMemDriver())
		ly, err := openLayer(ctx, layerParams{store: store, id: "punch-vs-full-block", config: cfg})
		require.NoError(t, err)

		var wg sync.WaitGroup
		start := make(chan struct{})
		var writeErr, punchErr error

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			writeErr = ly.Write(blockData, 0)
		}()
		go func() {
			defer wg.Done()
			<-start
			punchErr = ly.PunchHole(0, PageSize)
		}()

		close(start)
		wg.Wait()
		require.NoError(t, writeErr)
		require.NoError(t, punchErr)
		require.NoError(t, ly.Flush())

		got := make([]byte, BlockSize)
		_, err = ly.Read(ctx, got, 0)
		require.NoError(t, err)
		if !bytes.Equal(got, blockData) && !bytes.Equal(got, punchLast) {
			t.Fatalf("non-serializable result on iteration %d", i)
		}

		ly.Close()
	}
}

func TestLayerPartialWriteBlocksPublishCommit(t *testing.T) {
	ctx := t.Context()

	t.Run("flush", func(t *testing.T) {
		mem := blob.NewMemDriver()
		store := blob.New(mem)
		cfg := Config{
			FlushThreshold: 8 * PageSize,
			FlushInterval:  -1,
		}

		ly, err := openLayer(ctx, layerParams{store: store, id: "partial-blocks-flush", config: cfg})
		require.NoError(t, err)
		defer ly.Close()

		oldBlock := fillBlock(0xA1)
		require.NoError(t, ly.Write(oldBlock, 0))
		ly.mu.RLock()
		_, seq := ly.l2Map.Find(0)
		ly.mu.RUnlock()
		oldKey := blockKey(ly.id, "l2", seq, 0)
		ly.blockCache.clear()

		partialStarted := make(chan struct{})
		unblockPartial := make(chan struct{})
		var getCalls atomic.Int32
		mem.SetFault(blob.OpGet, oldKey, blob.Fault{
			Hook: func() {
				if getCalls.Add(1) != 1 {
					return
				}
				close(partialStarted)
				<-unblockPartial
			},
		})

		page1 := bytes.Repeat([]byte{0xA2}, PageSize)
		require.NoError(t, ly.Write(page1, PageSize))

		partialData := bytes.Repeat([]byte{0xA3}, 512)
		partialDone := make(chan error, 1)
		go func() {
			partialDone <- ly.Write(partialData, 0)
		}()

		<-partialStarted

		flushStarted := make(chan struct{})
		flushDone := make(chan error, 1)
		go func() {
			close(flushStarted)
			flushDone <- ly.Flush()
		}()

		<-flushStarted
		select {
		case err := <-flushDone:
			t.Fatalf("flush finished before partial write released: %v", err)
		default:
		}

		close(unblockPartial)
		require.NoError(t, <-partialDone)
		require.NoError(t, <-flushDone)
		require.NoError(t, ly.Flush())

		got := make([]byte, BlockSize)
		_, err = ly.Read(ctx, got, 0)
		require.NoError(t, err)

		want := append([]byte(nil), oldBlock...)
		copy(want[:len(partialData)], partialData)
		copy(want[PageSize:2*PageSize], page1)
		require.Equal(t, want, got)
	})

	t.Run("full-block-write", func(t *testing.T) {
		mem := blob.NewMemDriver()
		store := blob.New(mem)
		cfg := Config{
			FlushThreshold: 2 * BlockSize,
			FlushInterval:  -1,
		}

		ly, err := openLayer(ctx, layerParams{store: store, id: "partial-blocks-full-write", config: cfg})
		require.NoError(t, err)
		defer ly.Close()

		oldBlock := fillBlock(0xB1)
		require.NoError(t, ly.Write(oldBlock, 0))
		ly.mu.RLock()
		_, seq := ly.l2Map.Find(0)
		ly.mu.RUnlock()
		oldKey := blockKey(ly.id, "l2", seq, 0)
		ly.blockCache.clear()

		partialStarted := make(chan struct{})
		unblockPartial := make(chan struct{})
		var getCalls atomic.Int32
		mem.SetFault(blob.OpGet, oldKey, blob.Fault{
			Hook: func() {
				if getCalls.Add(1) != 1 {
					return
				}
				close(partialStarted)
				<-unblockPartial
			},
		})

		partialData := bytes.Repeat([]byte{0xB2}, 512)
		partialDone := make(chan error, 1)
		go func() {
			partialDone <- ly.Write(partialData, 0)
		}()

		<-partialStarted

		fullBlock := fillBlock(0xB3)
		writeStarted := make(chan struct{})
		writeDone := make(chan error, 1)
		go func() {
			close(writeStarted)
			writeDone <- ly.Write(fullBlock, 0)
		}()

		<-writeStarted
		select {
		case err := <-writeDone:
			t.Fatalf("full-block write finished before partial write released: %v", err)
		default:
		}

		close(unblockPartial)
		require.NoError(t, <-partialDone)
		require.NoError(t, <-writeDone)
		require.NoError(t, ly.Flush())

		got := make([]byte, BlockSize)
		_, err = ly.Read(ctx, got, 0)
		require.NoError(t, err)
		require.Equal(t, fullBlock[PageSize:], got[PageSize:])

		wantPartialPage := append([]byte(nil), oldBlock[:PageSize]...)
		copy(wantPartialPage[:len(partialData)], partialData)
		page0 := got[:PageSize]
		if !bytes.Equal(page0, oldBlock[:PageSize]) &&
			!bytes.Equal(page0, wantPartialPage) &&
			!bytes.Equal(page0, fullBlock[:PageSize]) {
			t.Fatalf("unexpected page 0 state after blocked publish")
		}
	})
}
