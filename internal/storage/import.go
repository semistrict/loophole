package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/semistrict/loophole/internal/blob"
	"github.com/semistrict/loophole/internal/env"
)

var maxImportWorkers = env.OptionInt("storage.maximportworkers", 64)

// CreateVolumeFromImage reads a raw image from r, splits it into L1/L2 blocks,
// uploads them in parallel, and creates a volume ref pointing at the new layer.
// The volume size is derived from the total bytes consumed from the reader.
func CreateVolumeFromImage(ctx context.Context, store *blob.Store, name string, volType string, r io.Reader) error {
	layerID := uuid.NewString()

	var cfg Config
	cfg.setDefaults()
	compress := !cfg.DisableCompression

	buf := make([]byte, BlockSize)
	var totalBytes uint64
	var blockAddr BlockIdx
	var seq atomic.Uint64
	seq.Store(1)

	var mu sync.Mutex
	var l1Ranges []blockRange
	var l2Ranges []blockRange

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxImportWorkers)

	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			totalBytes += uint64(n)
			data := buf[:n]

			// Pad to page boundary if needed.
			if rem := len(data) % PageSize; rem != 0 {
				padded := make([]byte, len(data)+(PageSize-rem))
				copy(padded, data)
				data = padded
			}

			if !isZeroBlock(data) {
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

				if len(pages) > 0 {
					// Capture loop variables for the goroutine.
					capturedAddr := blockAddr
					capturedPages := pages
					capturedSeq := seq.Add(1) - 1

					// Need a fresh copy of data since buf is reused.
					ownedPages := make([]blockPage, len(capturedPages))
					for i, p := range capturedPages {
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
			}
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read image: %w", readErr)
		}

		blockAddr++
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

	return nil
}
