// loophole-cached is the page cache daemon. It manages a shared mmap arena
// and serves page cache lookups over a UDS control plane.
//
// Typically started automatically by the first client that needs the cache.
// Can also be started manually:
//
//	loophole-cached --dir ~/.loophole/cache/myprofile/diskcache
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/semistrict/loophole/cached/cachedserver"
)

func main() {
	d := flag.String("dir", "", "cache directory (required)")
	flag.Parse()

	if *d == "" {
		slog.Error("--dir is required")
		os.Exit(1)
	}

	if err := cachedserver.StartServer(*d); err != nil {
		slog.Error("start daemon", "error", err)
		os.Exit(1)
	}

	slog.Info("loophole-cached started", "dir", *d)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down")
	if err := cachedserver.Shutdown(); err != nil {
		slog.Error("close daemon", "error", err)
		os.Exit(1)
	}
}
