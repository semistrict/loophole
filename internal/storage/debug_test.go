package storage

import (
	"context"
	"fmt"
	"strings"
)

// DebugPage traces through all layers for a given page address, showing what
// each layer holds. Used in test mismatch reports to diagnose read issues.
func (ly *layer) DebugPage(ctx context.Context, pageIdx PageIdx) string {
	isZero := func(b []byte) bool {
		for _, v := range b {
			if v != 0 {
				return false
			}
		}
		return true
	}
	hash := func(b []byte) string {
		if len(b) >= 4 {
			return fmt.Sprintf("%02x%02x%02x%02x", b[0], b[1], b[2], b[3])
		}
		return "?"
	}

	var lines []string
	add := func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	add("=== DebugPage layer=%s page=%d ===", ly.id[:8], pageIdx)

	ly.mu.RLock()
	l1map := ly.l1Map
	l2map := ly.l2Map
	mt := ly.active
	frozen := ly.pending
	ly.mu.RUnlock()

	// 1. Active dirty pages.
	if mt != nil {
		if rec, ok := mt.lookup(pageIdx); ok {
			add("  [1] dirty pages: zero=%v hash=%s", isZero(rec.bytes()), hash(rec.bytes()))
		} else {
			add("  [1] dirty pages: not present")
		}
	}

	// 2. Pending dirty batch.
	if frozen != nil {
		if rec, ok := frozen.lookup(pageIdx); ok {
			add("  [2] frozen: zero=%v hash=%s", isZero(rec.bytes()), hash(rec.bytes()))
		}
	}

	// 3. L1 (sparse blocks).
	block := pageIdx.Block()
	if layer, seq := l1map.Find(block); layer != "" {
		key := blockKey(layer, "l1", seq, block)
		data, found, err := ly.readFromBlock(ctx, "l1", layer, seq, pageIdx)
		if err != nil {
			add("  [3] L1 block=%d layer=%s seq=%d key=%s: err=%v", block, layer[:8], seq, key, err)
		} else if found {
			add("  [3] L1 block=%d layer=%s seq=%d key=%s: FOUND zero=%v hash=%s", block, layer[:8], seq, key, isZero(data), hash(data))
		} else {
			add("  [3] L1 block=%d layer=%s seq=%d key=%s: page not in block", block, layer[:8], seq, key)
		}
	} else {
		add("  [3] L1: no block for addr %d", block)
	}

	// 4. L2 (dense blocks).
	if layer, seq := l2map.Find(block); layer != "" {
		key := blockKey(layer, "l2", seq, block)
		data, found, err := ly.readFromBlock(ctx, "l2", layer, seq, pageIdx)
		if err != nil {
			add("  [4] L2 block=%d layer=%s seq=%d key=%s: err=%v", block, layer[:8], seq, key, err)
		} else if found {
			add("  [4] L2 block=%d layer=%s seq=%d key=%s: FOUND zero=%v hash=%s", block, layer[:8], seq, key, isZero(data), hash(data))
		} else {
			add("  [4] L2 block=%d layer=%s seq=%d key=%s: page not in block", block, layer[:8], seq, key)
		}
	} else {
		add("  [4] L2: no block for addr %d", block)
	}

	return strings.Join(lines, "\n")
}
