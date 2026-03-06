package filecmd

import (
	"context"
	"fmt"
	"io"
	"path"
)

func init() {
	Register(Command{
		Name:  "cp",
		Short: "Copy a file into or out of a volume",
		Usage: "cp <src> <dst>  (use volume:/path for volume paths)",
		Run:   runCp,
	})
}

func runCp(ctx context.Context, sess Session, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("cp: expected 2 arguments: <src> <dst>")
	}

	srcVol, srcPath, srcIsVol := ParseVolPath(args[0])
	dstVol, dstPath, dstIsVol := ParseVolPath(args[1])

	switch {
	case !srcIsVol && dstIsVol:
		// local -> volume
		return cpLocalToVol(ctx, sess, args[0], dstVol, dstPath)
	case srcIsVol && !dstIsVol:
		// volume -> local (write to stdout since we can't write local files from daemon)
		_ = srcPath
		return cpVolToStdout(ctx, sess, srcVol, srcPath)
	case srcIsVol && dstIsVol:
		_ = dstPath
		return fmt.Errorf("cp: volume-to-volume copy not supported")
	default:
		return fmt.Errorf("cp: at least one argument must be a volume:/path")
	}
}

func cpLocalToVol(ctx context.Context, sess Session, localPath, vol, volPath string) error {
	r, err := sess.Read(localPath)
	if err != nil {
		return fmt.Errorf("cp: open %s: %w", localPath, err)
	}
	defer func() { _ = r.Close() }()

	fsys, err := sess.FS(vol)
	if err != nil {
		return err
	}

	// Ensure parent directory exists.
	if dir := path.Dir(volPath); dir != "." && dir != "/" {
		if err := fsys.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("cp: mkdir %s: %w", dir, err)
		}
	}

	_ = fsys.Remove(volPath)
	f, err := fsys.Create(volPath)
	if err != nil {
		return fmt.Errorf("cp: create %s: %w", volPath, err)
	}
	_, copyErr := io.Copy(f, r)
	closeErr := f.Close()
	if copyErr != nil {
		return fmt.Errorf("cp: write %s: %w", volPath, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("cp: close %s: %w", volPath, closeErr)
	}
	return nil
}

func cpVolToStdout(ctx context.Context, sess Session, vol, volPath string) error {
	fsys, err := sess.FS(vol)
	if err != nil {
		return err
	}
	f, err := fsys.Open(volPath)
	if err != nil {
		return fmt.Errorf("cp: open %s:%s: %w", vol, volPath, err)
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(sess.Stdout(), f)
	return err
}
