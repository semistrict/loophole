package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/internal/env"
)

var globalPID int
var globalLogLevel string

func main() {
	root := rootCmd()
	if err := root.Execute(); err != nil {
		var exitErr *exitCodeError
		if errors.As(err, &exitErr) {
			if exitErr.msg != "" {
				fmt.Fprintln(os.Stderr, exitErr.msg)
			}
			os.Exit(exitErr.code)
		}
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "loophole",
		Short: "S3-backed FUSE filesystem with instant copy-on-write clones",
		Long:  "Loophole exposes a virtual block device backed by S3 with checkpoints and clones.",
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().IntVar(&globalPID, "pid", 0, "Connect to an embedded loophole daemon in the process with this PID")
	root.PersistentFlags().StringVar(&globalLogLevel, "log-level", "", "Set runtime log level (CLI default: warn; explicit flag or LOOPHOLE_LOG_LEVEL also affects daemon/file logging)")

	addCommands(root)

	return root
}

func resolveStore(rawURL string) (env.ResolvedStore, error) {
	return env.ResolveStore(rawURL, globalLogLevel)
}
