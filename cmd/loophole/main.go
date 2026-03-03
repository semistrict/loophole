package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/client"
)

var selfBin string
var globalProfile string

func main() {
	selfBin, _ = os.Executable()
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "loophole",
		Short: "S3-backed FUSE filesystem with instant copy-on-write clones",
		Long:  "Loophole exposes a virtual block device backed by S3 with instant snapshots and clones.",
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVarP(&globalProfile, "profile", "p", "default", "Named profile from ~/.loophole/config.toml")

	root.AddCommand(startCmd())
	addCommands(root)

	return root
}

func startCmd() *cobra.Command {
	var daemonize bool
	var socketMode uint32

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the loophole daemon",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := loophole.DefaultDir()
			inst, err := resolveProfile(dir)
			if err != nil {
				return err
			}

			running, err := checkExistingDaemon(dir, inst)
			if err != nil {
				return err
			}
			if running {
				return nil
			}

			if daemonize {
				return startDaemonBackground(inst, dir)
			}

			ctx := context.Background()
			return startDaemon(ctx, inst, dir, os.FileMode(socketMode))
		},
	}

	cmd.Flags().BoolVarP(&daemonize, "daemon", "d", false, "Run in background")
	cmd.Flags().Uint32Var(&socketMode, "socket-mode", 0, "Socket file permissions (e.g. 0666); 0 means use default umask")

	return cmd
}

// resolveProfile loads the config and resolves the current profile.
func resolveProfile(dir loophole.Dir) (loophole.Instance, error) {
	cfg, err := loophole.LoadConfig(dir)
	if err != nil {
		return loophole.Instance{}, err
	}
	return cfg.Resolve(globalProfile)
}

// checkExistingDaemon checks whether a daemon is already running for this profile.
func checkExistingDaemon(dir loophole.Dir, inst loophole.Instance) (running bool, err error) {
	sockPath := dir.Socket(inst.ProfileName)
	if _, err := os.Stat(sockPath); os.IsNotExist(err) {
		return false, nil
	}
	c := client.NewFromSocket(sockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, err := c.Status(ctx)
	if err != nil {
		return false, fmt.Errorf("stale socket at %s (rm it to start a new daemon)", sockPath)
	}
	fmt.Fprintf(os.Stderr, "loophole already running (%s)\n", status.S3)
	return true, nil
}
