package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/semistrict/loophole/client"
)

const chrootSocketPath = "/.loophole"

// inChroot returns true if running inside a loophole chroot (/.loophole socket exists).
func inChroot() bool {
	fi, err := os.Stat(chrootSocketPath)
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeSocket != 0
}

func chrootRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "loophole",
		Short: "Loophole volume control (chroot mode)",
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(
		chrootFlushCmd(),
		chrootSnapshotCmd(),
		chrootCloneCmd(),
		chrootStatusCmd(),
	)

	return root
}

func chrootClient() *client.Client {
	return client.NewFromSocket(chrootSocketPath)
}

func chrootFlushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "flush",
		Short: "Flush volume data to S3",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			if err := c.Flush(cmd.Context()); err != nil {
				return err
			}
			fmt.Println("flushed")
			return nil
		},
	}
}

func chrootSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot <name>",
		Short: "Create a snapshot of this volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			if err := c.ChrootSnapshot(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Printf("snapshot %q created\n", args[0])
			return nil
		},
	}
}

func chrootCloneCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clone <name>",
		Short: "Clone this volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			mp, err := c.ChrootClone(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			fmt.Printf("cloned to %s (mountpoint %s)\n", args[0], mp)
			return nil
		},
	}
}

func chrootStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show volume status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			c := chrootClient()
			s, err := c.ChrootStatus(cmd.Context())
			if err != nil {
				return err
			}
			// Fallback for old-style responses that only have "volume".
			if s.Name == "" && s.Volume != "" {
				fmt.Printf("volume: %s\n", s.Volume)
				return nil
			}
			printChrootStatus(s)
			return nil
		},
	}
}

func printChrootStatus(s *client.ChrootStatusResponse) {
	header := color.New(color.Bold, color.Underline)
	lbl := color.New(color.FgCyan)

	_, _ = header.Println("Volume")
	_, _ = lbl.Print("  name       ")
	fmt.Println(s.Name)
	_, _ = lbl.Print("  size       ")
	fmt.Println(humanBytes(float64(s.Size)))
	_, _ = lbl.Print("  type       ")
	fmt.Println(s.Type)
	_, _ = lbl.Print("  read-only  ")
	fmt.Println(s.ReadOnly)
	_, _ = lbl.Print("  refs       ")
	fmt.Println(s.Refs)
	fmt.Println()

	ly := s.Layer
	_, _ = header.Println("Layer")
	_, _ = lbl.Print("  id         ")
	id := ly.LayerID
	if len(id) > 12 {
		id = id[:12] + "..."
	}
	fmt.Println(id)
	fmt.Println()

	_, _ = header.Println("Memtable")
	_, _ = lbl.Print("  pages      ")
	fmt.Printf("%d / %d", ly.MemtablePages, ly.MemtableMax)
	if ly.MemtableMax > 0 {
		pct := float64(ly.MemtablePages) / float64(ly.MemtableMax) * 100
		fmt.Printf("  (%.0f%%)", pct)
	}
	fmt.Println()
	_, _ = lbl.Print("  size       ")
	fmt.Printf("%s / %s\n", humanBytes(float64(ly.MemtablePages)*4096), humanBytes(float64(ly.MemtableMax)*4096))
	_, _ = lbl.Print("  frozen     ")
	fmt.Println(ly.FrozenCount)
	fmt.Println()

	_, _ = header.Println("LSM")
	_, _ = lbl.Print("  L0 files   ")
	fmt.Println(ly.L0Count)
	_, _ = lbl.Print("  L0 pages   ")
	fmt.Println(ly.L0TotalPages)
	_, _ = lbl.Print("  L1 ranges  ")
	fmt.Println(ly.L1Ranges)
	_, _ = lbl.Print("  L2 ranges  ")
	fmt.Println(ly.L2Ranges)
	fmt.Println()

	_, _ = header.Println("Caches")
	_, _ = lbl.Print("  L0 cached  ")
	fmt.Println(ly.L0CacheEntries)
	_, _ = lbl.Print("  blk cached ")
	fmt.Println(ly.BlockCacheEnts)
	fmt.Println()
}
