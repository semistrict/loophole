//go:build !linux && !darwin

package main

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/semistrict/loophole"
)

func addPlatformCommands(_ *cobra.Command) {}

func startDaemon(_ context.Context, _ loophole.Instance, _ loophole.Dir, _ bool, _ os.FileMode, _ loophole.Mode, _ *loophole.S3Options) error {
	return fmt.Errorf("loophole daemon is not supported on %s", runtime.GOOS)
}

func startDaemonBackground(_ loophole.Instance, _ loophole.Dir, _ loophole.Mode, _ bool) error {
	return fmt.Errorf("loophole daemon is not supported on %s", runtime.GOOS)
}
