package util

import (
	"io"
	"log/slog"
)

// SafeClose closes c and logs a warning on error.
func SafeClose(c io.Closer, msg string) {
	if err := c.Close(); err != nil {
		slog.Warn(msg, "error", err)
	}
}
