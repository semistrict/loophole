package filecmd

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/semistrict/loophole/fsbackend"
)

func init() {
	Register(Command{
		Name:  "tar",
		Short: "Extract or create tar archives to/from volumes",
		Usage: "tar {-x|-c}[z][v] -C <volume>:<path> [-f <file>]",
		Run:   runTar,
	})
}

// tarOpts holds parsed tar flags.
type tarOpts struct {
	extract bool
	create  bool
	gzip    bool
	verbose bool
	volume  string
	dir     string
	file    string // -f filename (requested from client)
}

func parseTarFlags(args []string) (*tarOpts, error) {
	opts := &tarOpts{dir: "/"}

	i := 0
	for i < len(args) {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			return nil, fmt.Errorf("tar: unexpected argument %q", arg)
		}
		flags := arg[1:]
		for j := 0; j < len(flags); j++ {
			switch flags[j] {
			case 'x':
				opts.extract = true
			case 'c':
				opts.create = true
			case 'z':
				opts.gzip = true
			case 'v':
				opts.verbose = true
			case 'f':
				// -f takes the next argument (or rest of this flag group).
				if j+1 < len(flags) {
					opts.file = string(flags[j+1:])
					j = len(flags)
				} else {
					i++
					if i >= len(args) {
						return nil, fmt.Errorf("tar: -f requires an argument")
					}
					opts.file = args[i]
				}
			case 'C':
				i++
				if i >= len(args) {
					return nil, fmt.Errorf("tar: -C requires an argument")
				}
				vol, p, ok := ParseVolPath(args[i])
				if !ok {
					return nil, fmt.Errorf("tar: -C expects volume:/path, got %q", args[i])
				}
				opts.volume = vol
				opts.dir = p
			default:
				return nil, fmt.Errorf("tar: unknown flag -%c", flags[j])
			}
		}
		i++
	}

	if !opts.extract && !opts.create {
		return nil, fmt.Errorf("tar: must specify -x (extract) or -c (create)")
	}
	if opts.extract && opts.create {
		return nil, fmt.Errorf("tar: cannot specify both -x and -c")
	}
	if opts.volume == "" {
		return nil, fmt.Errorf("tar: -C volume:/path is required")
	}
	return opts, nil
}

func runTar(ctx context.Context, sess Session, args []string) error {
	opts, err := parseTarFlags(args)
	if err != nil {
		return err
	}

	fsys, err := sess.FS(opts.volume)
	if err != nil {
		return err
	}

	if opts.extract {
		return runTarExtract(ctx, sess, fsys, opts)
	}
	return runTarCreate(ctx, fsys, opts, sess.Stdout())
}

func runTarExtract(ctx context.Context, sess Session, fsys fsbackend.FS, opts *tarOpts) error {
	var input io.ReadCloser
	if opts.file != "" {
		r, err := sess.Read(opts.file)
		if err != nil {
			return fmt.Errorf("tar: open %s: %w", opts.file, err)
		}
		input = r
	} else {
		r, err := sess.Read("-") // stdin
		if err != nil {
			return fmt.Errorf("tar: read stdin: %w", err)
		}
		input = r
	}
	defer func() { _ = input.Close() }()

	return extractTar(ctx, fsys, opts, input, sess.Stdout())
}

func extractTar(ctx context.Context, fsys fsbackend.FS, opts *tarOpts, input io.Reader, output io.Writer) error {
	r := io.Reader(input)
	if opts.gzip {
		gr, err := gzip.NewReader(input)
		if err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		defer func() { _ = gr.Close() }()
		r = gr
	}

	tr := tar.NewReader(r)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read: %w", err)
		}

		name := cleanTarName(hdr.Name)
		if name == "" || name == "." {
			continue
		}
		fullPath := path.Join(opts.dir, name)

		if opts.verbose {
			_, _ = fmt.Fprintln(output, name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := fsys.MkdirAll(fullPath, fs.FileMode(hdr.Mode)&0o7777); err != nil {
				return fmt.Errorf("mkdir %s: %w", name, err)
			}
			if err := fsys.Chmod(fullPath, fs.FileMode(hdr.Mode)&0o7777); err != nil {
				return fmt.Errorf("chmod %s: %w", name, err)
			}

		case tar.TypeReg:
			if dir := path.Dir(fullPath); dir != "." && dir != "/" {
				if err := fsys.MkdirAll(dir, 0o755); err != nil {
					return fmt.Errorf("mkdir parent %s: %w", dir, err)
				}
			}
			_ = fsys.Remove(fullPath)
			f, err := fsys.Create(fullPath)
			if err != nil {
				return fmt.Errorf("create %s: %w", name, err)
			}
			_, err = io.Copy(f, tr)
			closeErr := f.Close()
			if err != nil {
				return fmt.Errorf("write %s: %w", name, err)
			}
			if closeErr != nil {
				return fmt.Errorf("close %s: %w", name, closeErr)
			}
			if err := fsys.Chmod(fullPath, fs.FileMode(hdr.Mode)&0o7777); err != nil {
				return fmt.Errorf("chmod %s: %w", name, err)
			}

		case tar.TypeSymlink:
			if dir := path.Dir(fullPath); dir != "." && dir != "/" {
				if err := fsys.MkdirAll(dir, 0o755); err != nil {
					return fmt.Errorf("mkdir parent %s: %w", dir, err)
				}
			}
			_ = fsys.Remove(fullPath)
			if err := fsys.Symlink(hdr.Linkname, fullPath); err != nil {
				return fmt.Errorf("symlink %s -> %s: %w", name, hdr.Linkname, err)
			}

		default:
			continue
		}

		_ = fsys.Lchown(fullPath, hdr.Uid, hdr.Gid)
		if hdr.Typeflag != tar.TypeSymlink && !hdr.ModTime.IsZero() {
			_ = fsys.Chtimes(fullPath, hdr.ModTime.Unix())
		}
	}
	return nil
}

func runTarCreate(ctx context.Context, fsys fsbackend.FS, opts *tarOpts, output io.Writer) error {
	w := io.Writer(output)
	if opts.gzip {
		gw := gzip.NewWriter(output)
		defer func() { _ = gw.Close() }()
		w = gw
	}

	tw := tar.NewWriter(w)
	defer func() { _ = tw.Close() }()

	return walkDir(ctx, fsys, opts.dir, ".", opts.verbose, output, tw)
}

func walkDir(ctx context.Context, fsys fsbackend.FS, root, rel string, verbose bool, verboseOut io.Writer, tw *tar.Writer) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	fullPath := root
	if rel != "." {
		fullPath = path.Join(root, rel)
	}

	names, err := fsys.ReadDir(fullPath)
	if err != nil {
		return err
	}

	for _, name := range names {
		entryRel := name
		if rel != "." {
			entryRel = rel + "/" + name
		}
		entryFull := path.Join(root, entryRel)

		info, err := fsys.Lstat(entryFull)
		if err != nil {
			return err
		}

		if verbose {
			_, _ = fmt.Fprintln(verboseOut, entryRel)
		}

		mode := info.Mode()
		switch {
		case mode.IsDir():
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeDir,
				Name:     entryRel + "/",
				Mode:     int64(mode.Perm()),
				ModTime:  info.ModTime(),
			}); err != nil {
				return err
			}
			if err := walkDir(ctx, fsys, root, entryRel, verbose, verboseOut, tw); err != nil {
				return err
			}

		case mode&fs.ModeSymlink != 0:
			target, err := fsys.Readlink(entryFull)
			if err != nil {
				return err
			}
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     entryRel,
				Linkname: target,
				Mode:     int64(mode.Perm()),
				ModTime:  info.ModTime(),
			}); err != nil {
				return err
			}

		case mode.IsRegular():
			if err := tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,
				Name:     entryRel,
				Size:     info.Size(),
				Mode:     int64(mode.Perm()),
				ModTime:  info.ModTime(),
			}); err != nil {
				return err
			}
			f, err := fsys.Open(entryFull)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(tw, f)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	return nil
}

// cleanTarName normalizes a tar entry name.
func cleanTarName(name string) string {
	name = strings.TrimPrefix(name, "./")
	name = strings.TrimSuffix(name, "/")
	return path.Clean(name)
}
