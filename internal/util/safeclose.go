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

// SafeRun calls fn and logs a warning on error.
func SafeRun(fn func() error, msg string) {
	if err := fn(); err != nil {
		slog.Warn(msg, "error", err)
	}
}

// SafeRunCtx calls fn with ctx and logs a warning on error.
func SafeRunCtx(ctx context.Context, fn func(context.Context) error, msg string) {
	if err := fn(ctx); err != nil {
		slog.Warn(msg, "error", err)
	}
}
