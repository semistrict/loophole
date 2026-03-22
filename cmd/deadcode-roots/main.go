// Command deadcode-roots is a synthetic entry point that references internal
// functions only reachable from cgo //export entry points (package capi).
// It exists solely so that `deadcode` can trace those call paths.
// It is never built or shipped — only loaded by the deadcode analyser.
package main

import (
	"context"

	"github.com/semistrict/loophole/internal/fsserver"
	"github.com/semistrict/loophole/internal/storage"
	"github.com/semistrict/loophole/internal/volserver"
)

func main() {
	// capi calls volserver.Start → .Serve / .Close
	srv, _ := volserver.Start(nil, "")
	_ = srv.Serve(context.TODO())
	srv.Close()

	// capi calls fsserver.StartEmbedded (which wires the above)
	_, _ = fsserver.StartEmbedded((*storage.Volume)(nil))
}
