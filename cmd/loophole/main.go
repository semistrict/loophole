package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/env"
)

var globalProfile string
var globalPID int

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

	root.PersistentFlags().StringVarP(&globalProfile, "profile", "p", "", "Named profile (default: default_profile from config, or first defined)")
	root.PersistentFlags().IntVar(&globalPID, "pid", 0, "Connect to an embedded loophole daemon in the process with this PID")

	addCommands(root)

	return root
}

// resolveProfile loads the config and resolves the current profile.
func resolveProfile(dir env.Dir) (env.ResolvedProfile, error) {
	cfg, err := env.LoadConfig(dir)
	if err != nil {
		return env.ResolvedProfile{}, err
	}
	return cfg.Resolve(globalProfile)
}
