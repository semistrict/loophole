package filecmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

func init() {
	Register(Command{
		Name:  "git-clone",
		Short: "Clone a git repository into a volume",
		Usage: "git-clone [--branch <branch>] [--depth <n>] <url> <volume>:<path>",
		Run:   runGitClone,
	})
}

type gitCloneOpts struct {
	url    string
	volume string
	dir    string
	branch string
	depth  int
}

func parseGitCloneArgs(args []string) (*gitCloneOpts, error) {
	opts := &gitCloneOpts{}

	i := 0
	var positional []string
	for i < len(args) {
		switch args[i] {
		case "--branch", "-b":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("git-clone: --branch requires an argument")
			}
			opts.branch = args[i]
		case "--depth":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("git-clone: --depth requires an argument")
			}
			n := 0
			for _, c := range args[i] {
				if c < '0' || c > '9' {
					return nil, fmt.Errorf("git-clone: --depth must be a number")
				}
				n = n*10 + int(c-'0')
			}
			opts.depth = n
		default:
			if strings.HasPrefix(args[i], "-") {
				return nil, fmt.Errorf("git-clone: unknown flag %q", args[i])
			}
			positional = append(positional, args[i])
		}
		i++
	}

	if len(positional) != 2 {
		return nil, fmt.Errorf("git-clone: expected <url> <volume>:<path>")
	}

	opts.url = positional[0]
	vol, p, ok := ParseVolPath(positional[1])
	if !ok {
		return nil, fmt.Errorf("git-clone: destination must be volume:/path, got %q", positional[1])
	}
	opts.volume = vol
	opts.dir = p

	return opts, nil
}

func runGitClone(ctx context.Context, sess Session, args []string) error {
	opts, err := parseGitCloneArgs(args)
	if err != nil {
		return err
	}

	fsys, err := sess.FS(opts.volume)
	if err != nil {
		return err
	}

	// Create target directory.
	if err := fsys.MkdirAll(opts.dir, 0o755); err != nil {
		return fmt.Errorf("git-clone: mkdir %s: %w", opts.dir, err)
	}

	worktree := newBillyFS(fsys, opts.dir)
	dotgit, err := worktree.Chroot(".git")
	if err != nil {
		return fmt.Errorf("git-clone: chroot .git: %w", err)
	}

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})

	cloneOpts := &git.CloneOptions{
		URL: opts.url,
	}
	if opts.branch != "" {
		cloneOpts.ReferenceName = plumbing.NewBranchReferenceName(opts.branch)
		cloneOpts.SingleBranch = true
	}
	if opts.depth > 0 {
		cloneOpts.Depth = opts.depth
	}

	stdout := sess.Stdout()
	cloneOpts.Progress = stdout

	_, err = git.CloneContext(ctx, storage, worktree, cloneOpts)
	if err != nil {
		return fmt.Errorf("git-clone: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "Cloned %s into %s:%s\n", opts.url, opts.volume, opts.dir)
	return nil
}
