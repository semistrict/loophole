package filecmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

var timeNow = time.Now

func init() {
	Register(Command{
		Name:  "git",
		Short: "Git operations on volumes (clone, checkout, log, status, add, commit)",
		Usage: "git [-C volume:/path] <subcommand> [args...]",
		Run:   runGit,
	})
}

// gitContext holds the parsed -C flag and remaining args.
type gitContext struct {
	volume  string
	repoDir string
	subcmd  string
	args    []string
}

func parseGitContext(args []string) (*gitContext, error) {
	gc := &gitContext{}

	i := 0
	for i < len(args) {
		if args[i] == "-C" {
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("git: -C requires volume:/path")
			}
			vol, p, ok := ParseVolPath(args[i])
			if !ok {
				return nil, fmt.Errorf("git: -C expects volume:/path, got %q", args[i])
			}
			gc.volume = vol
			gc.repoDir = p
			i++
			continue
		}
		break
	}

	if i >= len(args) {
		return nil, fmt.Errorf("git: missing subcommand (clone, checkout, log, status, add, commit)")
	}

	gc.subcmd = args[i]
	gc.args = args[i+1:]
	return gc, nil
}

// openRepo opens an existing git repository at volume:repoDir.
func openRepo(sess Session, volume, repoDir string) (*git.Repository, *billyFS, error) {
	fsys, err := sess.FS(volume)
	if err != nil {
		return nil, nil, err
	}

	worktree := newBillyFS(fsys, repoDir)
	dotgit, err := worktree.Chroot(".git")
	if err != nil {
		return nil, nil, fmt.Errorf("chroot .git: %w", err)
	}

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})
	repo, err := git.Open(storage, worktree)
	if err != nil {
		return nil, nil, err
	}
	return repo, worktree, nil
}

func runGit(ctx context.Context, sess Session, args []string) error {
	gc, err := parseGitContext(args)
	if err != nil {
		return err
	}

	switch gc.subcmd {
	case "clone":
		return runGitClone(ctx, sess, gc, gc.args)
	case "checkout":
		return runGitCheckout(ctx, sess, gc)
	case "log":
		return runGitLog(ctx, sess, gc)
	case "status":
		return runGitStatus(ctx, sess, gc)
	case "add":
		return runGitAdd(ctx, sess, gc)
	case "commit":
		return runGitCommit(ctx, sess, gc)
	default:
		return fmt.Errorf("git: unknown subcommand %q", gc.subcmd)
	}
}

func requireRepo(gc *gitContext) error {
	if gc.volume == "" {
		return fmt.Errorf("git %s: -C volume:/path is required", gc.subcmd)
	}
	return nil
}

// --- clone ---

func runGitClone(ctx context.Context, sess Session, gc *gitContext, args []string) error {
	var branch string
	var depth int
	var positional []string

	i := 0
	for i < len(args) {
		switch args[i] {
		case "--branch", "-b":
			i++
			if i >= len(args) {
				return fmt.Errorf("git clone: --branch requires an argument")
			}
			branch = args[i]
		case "--depth":
			i++
			if i >= len(args) {
				return fmt.Errorf("git clone: --depth requires an argument")
			}
			n := 0
			for _, c := range args[i] {
				if c < '0' || c > '9' {
					return fmt.Errorf("git clone: --depth must be a number")
				}
				n = n*10 + int(c-'0')
			}
			depth = n
		default:
			if strings.HasPrefix(args[i], "-") {
				return fmt.Errorf("git clone: unknown flag %q", args[i])
			}
			positional = append(positional, args[i])
		}
		i++
	}

	// clone accepts: <url-or-path> [<volume:path | /local/path>]
	// If -C was given, source is the only positional.
	// If -C was not given, we need <source> <destination>.
	var source, dest string
	switch len(positional) {
	case 1:
		if gc.volume == "" {
			return fmt.Errorf("git clone: expected <source> <dest>, or use -C volume:/path <source>")
		}
		source = positional[0]
		dest = gc.volume + ":" + gc.repoDir
	case 2:
		source = positional[0]
		dest = positional[1]
	default:
		return fmt.Errorf("git clone: expected <source> [<dest>]")
	}

	var worktree billy.Filesystem
	if vol, dir, ok := ParseVolPath(dest); ok {
		// Destination is a volume.
		fsys, err := sess.FS(vol)
		if err != nil {
			return err
		}
		if err := fsys.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("git clone: mkdir %s: %w", dir, err)
		}
		worktree = newBillyFS(fsys, dir)
	} else {
		// Destination is a local filesystem path.
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return fmt.Errorf("git clone: mkdir %s: %w", dest, err)
		}
		worktree = osfs.New(dest)
	}

	dotgit, err := worktree.Chroot(".git")
	if err != nil {
		return fmt.Errorf("git clone: chroot .git: %w", err)
	}

	storage := filesystem.NewStorageWithOptions(dotgit, cache.NewObjectLRUDefault(), filesystem.Options{})

	cloneOpts := &git.CloneOptions{URL: source}
	if branch != "" {
		cloneOpts.ReferenceName = plumbing.NewBranchReferenceName(branch)
		cloneOpts.SingleBranch = true
	}
	if depth > 0 {
		cloneOpts.Depth = depth
	}

	stdout := sess.Stdout()
	cloneOpts.Progress = stdout

	_, err = git.CloneContext(ctx, storage, worktree, cloneOpts)
	if err != nil {
		return fmt.Errorf("git clone: %w", err)
	}

	_, _ = fmt.Fprintf(stdout, "Cloned %s into %s\n", source, dest)
	return nil
}

// --- checkout ---

func runGitCheckout(ctx context.Context, sess Session, gc *gitContext) error {
	if err := requireRepo(gc); err != nil {
		return err
	}
	if len(gc.args) != 1 {
		return fmt.Errorf("git checkout: expected <ref>")
	}
	ref := gc.args[0]

	repo, _, err := openRepo(sess, gc.volume, gc.repoDir)
	if err != nil {
		return fmt.Errorf("git checkout: open repo: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("git checkout: %w", err)
	}

	// Try as branch first, then tag, then raw hash.
	opts := &git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(ref)}
	if err := wt.Checkout(opts); err != nil {
		opts.Branch = plumbing.NewTagReferenceName(ref)
		if err2 := wt.Checkout(opts); err2 != nil {
			// Try as commit hash.
			hash := plumbing.NewHash(ref)
			opts2 := &git.CheckoutOptions{Hash: hash}
			if err3 := wt.Checkout(opts2); err3 != nil {
				return fmt.Errorf("git checkout: %w", err)
			}
		}
	}

	_, _ = fmt.Fprintf(sess.Stdout(), "Checked out %s\n", ref)
	return nil
}

// --- log ---

func runGitLog(ctx context.Context, sess Session, gc *gitContext) error {
	if err := requireRepo(gc); err != nil {
		return err
	}

	count := 10
	i := 0
	for i < len(gc.args) {
		switch gc.args[i] {
		case "-n":
			i++
			if i >= len(gc.args) {
				return fmt.Errorf("git log: -n requires a number")
			}
			n := 0
			for _, c := range gc.args[i] {
				if c < '0' || c > '9' {
					return fmt.Errorf("git log: -n must be a number")
				}
				n = n*10 + int(c-'0')
			}
			count = n
		default:
			return fmt.Errorf("git log: unknown flag %q", gc.args[i])
		}
		i++
	}

	repo, _, err := openRepo(sess, gc.volume, gc.repoDir)
	if err != nil {
		return fmt.Errorf("git log: open repo: %w", err)
	}

	logIter, err := repo.Log(&git.LogOptions{})
	if err != nil {
		return fmt.Errorf("git log: %w", err)
	}
	defer logIter.Close()

	out := sess.Stdout()
	shown := 0
	for shown < count {
		commit, err := logIter.Next()
		if err != nil {
			break
		}
		_, _ = fmt.Fprintf(out, "commit %s\n", commit.Hash)
		_, _ = fmt.Fprintf(out, "Author: %s <%s>\n", commit.Author.Name, commit.Author.Email)
		_, _ = fmt.Fprintf(out, "Date:   %s\n", commit.Author.When.Format("Mon Jan 2 15:04:05 2006 -0700"))
		_, _ = fmt.Fprintf(out, "\n    %s\n\n", firstLine(commit.Message))
		shown++
	}
	return nil
}

// --- status ---

func runGitStatus(ctx context.Context, sess Session, gc *gitContext) error {
	if err := requireRepo(gc); err != nil {
		return err
	}

	repo, _, err := openRepo(sess, gc.volume, gc.repoDir)
	if err != nil {
		return fmt.Errorf("git status: open repo: %w", err)
	}

	head, err := repo.Head()
	if err != nil {
		return fmt.Errorf("git status: %w", err)
	}

	out := sess.Stdout()
	_, _ = fmt.Fprintf(out, "On branch %s\n", head.Name().Short())

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("git status: %w", err)
	}

	status, err := wt.Status()
	if err != nil {
		return fmt.Errorf("git status: %w", err)
	}

	if status.IsClean() {
		_, _ = io.WriteString(out, "nothing to commit, working tree clean\n")
		return nil
	}

	for path, s := range status {
		staging := s.Staging
		worktreeStatus := s.Worktree
		if staging == git.Unmodified {
			staging = ' '
		}
		if worktreeStatus == git.Unmodified {
			worktreeStatus = ' '
		}
		_, _ = fmt.Fprintf(out, "%c%c %s\n", staging, worktreeStatus, path)
	}
	return nil
}

// --- add ---

func runGitAdd(ctx context.Context, sess Session, gc *gitContext) error {
	if err := requireRepo(gc); err != nil {
		return err
	}
	if len(gc.args) == 0 {
		return fmt.Errorf("git add: expected <path>")
	}

	repo, _, err := openRepo(sess, gc.volume, gc.repoDir)
	if err != nil {
		return fmt.Errorf("git add: open repo: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("git add: %w", err)
	}

	for _, p := range gc.args {
		if _, err := wt.Add(p); err != nil {
			return fmt.Errorf("git add %s: %w", p, err)
		}
	}

	_, _ = fmt.Fprintf(sess.Stdout(), "staged %d path(s)\n", len(gc.args))
	return nil
}

// --- commit ---

func runGitCommit(ctx context.Context, sess Session, gc *gitContext) error {
	if err := requireRepo(gc); err != nil {
		return err
	}

	var message, authorName, authorEmail string
	i := 0
	for i < len(gc.args) {
		switch gc.args[i] {
		case "-m":
			i++
			if i >= len(gc.args) {
				return fmt.Errorf("git commit: -m requires a message")
			}
			message = gc.args[i]
		case "--author":
			i++
			if i >= len(gc.args) {
				return fmt.Errorf("git commit: --author requires 'Name <email>'")
			}
			authorName, authorEmail = parseAuthor(gc.args[i])
		default:
			return fmt.Errorf("git commit: unknown flag %q", gc.args[i])
		}
		i++
	}

	if message == "" {
		return fmt.Errorf("git commit: -m is required")
	}

	repo, _, err := openRepo(sess, gc.volume, gc.repoDir)
	if err != nil {
		return fmt.Errorf("git commit: open repo: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("git commit: %w", err)
	}

	opts := &git.CommitOptions{}
	if authorName != "" {
		opts.Author = &object.Signature{
			Name:  authorName,
			Email: authorEmail,
			When:  timeNow(),
		}
	}

	hash, err := wt.Commit(message, opts)
	if err != nil {
		return fmt.Errorf("git commit: %w", err)
	}

	_, _ = fmt.Fprintf(sess.Stdout(), "[%s] %s\n", hash.String()[:7], firstLine(message))
	return nil
}

// parseAuthor parses "Name <email>" into name and email.
func parseAuthor(s string) (string, string) {
	lt := strings.IndexByte(s, '<')
	gt := strings.IndexByte(s, '>')
	if lt >= 0 && gt > lt {
		return strings.TrimSpace(s[:lt]), s[lt+1 : gt]
	}
	return s, ""
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
