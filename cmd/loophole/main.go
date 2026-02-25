package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/nbdserve"
)

var selfBin string

func main() {
	selfBin, _ = os.Executable()
	if err := rootCmd().Execute(); err != nil {
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
		SilenceUsage: true,
	}

	root.AddCommand(startCmd())
	addPlatformCommands(root)

	return root
}

func startCmd() *cobra.Command {
	var daemonize bool
	var debug bool
	var unprivileged bool
	var socketMode uint32
	var nbdAddr string

	cmd := &cobra.Command{
		Use:   "start <s3url>",
		Short: "Start the loophole daemon",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inst, err := loophole.ParseInstance(args[0])
			if err != nil {
				return err
			}
			dir := loophole.DefaultDir()

			var mode loophole.Mode
			if unprivileged {
				mode = loophole.ModeLwext4FUSE
			}

			if nbdAddr != "" {
				return startWithNBD(inst, dir, debug, nbdAddr)
			}

			if daemonize {
				return startDaemonBackground(inst, dir, mode, unprivileged)
			}

			ctx := context.Background()
			return startDaemon(ctx, inst, dir, debug, os.FileMode(socketMode), mode, nil)
		},
	}

	cmd.Flags().BoolVarP(&daemonize, "daemon", "d", false, "Run in background")
	cmd.Flags().BoolVarP(&unprivileged, "unprivileged", "u", false, "Use FUSE+lwext4 (no root required)")
	cmd.Flags().BoolVar(&debug, "debug", false, "Enable debug logging")
	cmd.Flags().Uint32Var(&socketMode, "socket-mode", 0, "Socket file permissions (e.g. 0666); 0 means use default umask")
	cmd.Flags().StringVar(&nbdAddr, "nbd", "", "Serve volumes over NBD on this address (e.g. 127.0.0.1:10809 or /tmp/nbd.sock)")

	return cmd
}

// startWithNBD starts the NBD TCP/Unix server.
func startWithNBD(inst loophole.Instance, dir loophole.Dir, debug bool, nbdAddr string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	vm, err := loophole.SetupVolumeManager(ctx, inst, dir, nil, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := vm.Close(context.Background()); err != nil {
			logger.Warn("failed to close volume manager", "error", err)
		}
	}()

	nbdSrv := nbdserve.NewServer(vm, 0)

	network, addr := parseNBDAddr(nbdAddr)
	logger.Info("starting NBD server", "network", network, "addr", addr)

	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return nbdSrv.Serve(egCtx, network, addr)
	})

	fmt.Fprintf(os.Stderr, "NBD server listening on %s %s\n", network, addr)

	return eg.Wait()
}

// parseNBDAddr determines network type from the address string.
// If the address contains "/" or starts with ".", it's treated as a Unix socket.
// Otherwise it's TCP. A bare port like "10809" gets ":" prepended.
func parseNBDAddr(addr string) (network, resolved string) {
	if strings.Contains(addr, "/") || strings.HasPrefix(addr, ".") {
		return "unix", addr
	}
	// TCP address. Add default port if needed.
	_, _, err := net.SplitHostPort(addr)
	if err != nil {
		// No port specified — could be just a host or just a port number.
		if _, _, err2 := net.SplitHostPort(":" + addr); err2 == nil {
			// It was just a port number.
			return "tcp", ":" + addr
		}
		// It was just a host — add default NBD port.
		return "tcp", addr + ":10809"
	}
	return "tcp", addr
}
