package util

import (
	"context"
	"io"
	"log/slog"
)

// SafeClose closes c and logs a warning on error.
func SafeClose(c io.Closer, msg string) {
	if err := c.Close(); err != nil {
		slog.Warn(msg, "error", err)
	}
}

// ContextCloser is an interface for types whose Close requires a context.
type ContextCloser interface {
	Close(ctx context.Context) error
}

// SafeCloseContext closes c with the given context and logs a warning on error.
func SafeCloseContext(c ContextCloser, ctx context.Context, msg string) {
	if err := c.Close(ctx); err != nil {
		slog.Warn(msg, "error", err)
	}
}
