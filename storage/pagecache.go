package storage

import (
	"io"

	"github.com/semistrict/loophole/internal/safepoint"
)

// PageCache is the interface used by the storage layer to cache immutable
// pages. The canonical implementation is cached.PageCache (backed by the
// loophole-cached daemon). Passing nil disables persistent caching.
type PageCache interface {
	io.Closer
	GetPage(layerID string, pageIdx uint64) []byte
	GetPageRef(g safepoint.Guard, layerID string, pageIdx uint64) []byte
	PutPage(layerID string, pageIdx uint64, data []byte)
}
