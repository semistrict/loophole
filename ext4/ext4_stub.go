//go:build !linux

package ext4

import (
	"context"
	"fmt"
	"runtime"
)

func Format(_ context.Context, _ string) error {
	return fmt.Errorf("ext4 format requires Linux (got %s)", runtime.GOOS)
}

func FormatDirect(_ context.Context, _ string) error {
	return fmt.Errorf("ext4 format requires Linux (got %s)", runtime.GOOS)
}

func Mount(_ context.Context, _, _ string) (string, error) {
	return "", fmt.Errorf("ext4 mount requires Linux (got %s)", runtime.GOOS)
}

func LosetupDetach(_ context.Context, _ string) {}

func Unmount(_ context.Context, _ string) error {
	return fmt.Errorf("ext4 unmount requires Linux (got %s)", runtime.GOOS)
}

func Freeze(_ context.Context, _ string) error {
	return fmt.Errorf("fsfreeze requires Linux (got %s)", runtime.GOOS)
}

func Thaw(_ context.Context, _ string) error {
	return fmt.Errorf("fsthaw requires Linux (got %s)", runtime.GOOS)
}

func MountDirect(_ context.Context, _, _ string) error {
	return fmt.Errorf("ext4 direct mount requires Linux (got %s)", runtime.GOOS)
}

func UnmountDirect(_ context.Context, _ string) error {
	return fmt.Errorf("ext4 direct unmount requires Linux (got %s)", runtime.GOOS)
}

func IsMounted(_ string) bool {
	return false
}
