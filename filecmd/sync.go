package filecmd

import (
	"context"
	"fmt"
)

func init() {
	Register(Command{
		Name:  "sync",
		Short: "Flush a volume's data to persistent storage",
		Usage: "sync <volume>",
		Run:   runSync,
	})
}

func runSync(ctx context.Context, sess Session, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: sync <volume>")
	}
	volume := args[0]
	// Ensure volume is open by requesting its FS.
	if _, err := sess.FS(volume); err != nil {
		return err
	}
	return sess.Sync(volume)
}
