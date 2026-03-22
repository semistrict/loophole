package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/semistrict/loophole/internal/blob"
	"github.com/semistrict/loophole/internal/env"
)

var maxImportWorkers = env.OptionInt("storage.maximportworkers", 64)

// CreateVolumeFromImage reads a raw image from f, splits it into L1/L2 blocks,
// uploads them in parallel, and creates a volume ref pointing at the new layer.
// If the file is sparse, holes are skipped via SEEK_DATA/SEEK_HOLE.
// The volume size is derived from the file size.
func CreateVolumeFromImage(ctx context.Context, store *blob.Store, name string, volType string, f *os.File) error {
	layerID := uuid.NewString()

	var cfg Config
	cfg.setDefaults()
	compress := !cfg.DisableCompression

	iter, err := newSparseBlockIter(f, BlockSize)
	if err != nil {
		return fmt.Errorf("init sparse reader: %w", err)
	}
	totalBytes := uint64(iter.fileSize)

	slog.Info("image import: starting",
		"file_size", totalBytes,
		"sparse", iter.sparse,
	)

	var seq atomic.Uint64
	seq.Store(1)

	var mu sync.Mutex
	var l1Ranges []blockRange
	var l2Ranges []blockRange

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxImportWorkers)

	buf := make([]byte, BlockSize)
	var blocksRead, blocksSkipped uint64

	for {
		blockAddr, n, readErr := iter.next(buf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read image: %w", readErr)
		}

		data := buf[:n]

		// Pad to page boundary if needed.
		if rem := len(data) % PageSize; rem != 0 {
			padded := make([]byte, len(data)+(PageSize-rem))
			copy(padded, data)
			data = padded
		}

		blocksRead++

		if isZeroBlock(data) {
			blocksSkipped++
			continue
		}

		// Collect non-zero pages.
		var pages []blockPage
		for i := 0; i < len(data)/PageSize; i++ {
			page := data[i*PageSize : (i+1)*PageSize]
			if !isZeroBlock(page) {
				pages = append(pages, blockPage{
					offset: uint16(i),
					data:   page,
				})
			}
		}

		if len(pages) == 0 {
			blocksSkipped++
			continue
		}

		// Capture loop variables for the goroutine.
		capturedAddr := blockAddr
		capturedSeq := seq.Add(1) - 1

		// Need a fresh copy of data since buf is reused.
		ownedPages := make([]blockPage, len(pages))
		for i, p := range pages {
			owned := make([]byte, PageSize)
			copy(owned, p.data)
			ownedPages[i] = blockPage{offset: p.offset, data: owned}
		}

		promote := len(ownedPages) >= L1PromoteThreshold
		level := "l1"
		if promote {
			level = "l2"
		}

		g.Go(func() error {
			_, _, err := buildAndUploadBlock(gctx, store, layerID, level, capturedSeq,
				capturedAddr, nil, ownedPages, compress)
			if err != nil {
				return err
			}

			br := blockRange{
				Start:         capturedAddr,
				End:           capturedAddr + 1,
				Layer:         layerID,
				WriteLeaseSeq: capturedSeq,
			}

			mu.Lock()
			if promote {
				l2Ranges = append(l2Ranges, br)
			} else {
				l1Ranges = append(l1Ranges, br)
			}
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("upload blocks: %w", err)
	}

	// Write layer index.
	idx := layerIndex{
		NextSeq:   seq.Load(),
		LayoutGen: 1,
		L1:        l1Ranges,
		L2:        l2Ranges,
	}
	idxData, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal layer index: %w", err)
	}
	layerStore := store.At("layers/" + layerID)
	if err := layerStore.PutIfNotExists(ctx, "index.json", idxData, map[string]string{
		"created_at": time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		return fmt.Errorf("create layer index: %w", err)
	}

	slog.Info("image import: layer created",
		"layer", layerID,
		"l1_ranges", len(l1Ranges),
		"l2_ranges", len(l2Ranges),
		"total_bytes", totalBytes,
		"blocks_read", blocksRead,
		"blocks_skipped", blocksSkipped,
		"sparse", iter.sparse,
	)

	// Write volume ref.
	ref := volumeRef{
		LayerID: layerID,
		Size:    totalBytes,
		Type:    volType,
	}
	volRefs := store.At("volumes")
	if err := putVolumeRefNew(ctx, volRefs, name, ref); err != nil {
		return err
	}

	// Create an initial checkpoint so the volume can be cloned immediately.
	cpID, err := putCheckpoint(ctx, volRefs, name, layerID)
	if err != nil {
		return fmt.Errorf("create initial checkpoint: %w", err)
	}
	slog.Info("image import: checkpoint created", "volume", name, "checkpoint", cpID)

	return nil
}
