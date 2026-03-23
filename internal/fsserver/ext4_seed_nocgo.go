//go:build !cgo

package fsserver

import (
	"context"
	"fmt"
)

// BuildExt4ImageFromPath is unavailable without CGo (requires embedded libext2fs).
func BuildExt4ImageFromPath(_ context.Context, _ string, _ uint64) (string, error) {
	return "", fmt.Errorf("--mkfs requires CGo (embedded libext2fs); rebuild without CGO_ENABLED=0")
}
