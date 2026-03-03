package filecmd

import (
	"context"
	"fmt"
	"io"
)

func init() {
	Register(Command{
		Name:  "cat",
		Short: "Print a file from a volume",
		Usage: "cat <volume>:<path>",
		Run: func(ctx context.Context, sess Session, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("cat: expected 1 argument (volume:/path)")
			}
			vol, path, ok := ParseVolPath(args[0])
			if !ok {
				return fmt.Errorf("cat: expected volume:/path, got %q", args[0])
			}
			fsys, err := sess.FS(vol)
			if err != nil {
				return err
			}
			f, err := fsys.Open(path)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			_, err = io.Copy(sess.Stdout(), f)
			return err
		},
	})
}
