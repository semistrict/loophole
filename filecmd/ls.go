package filecmd

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"time"
)

func init() {
	Register(Command{
		Name:  "ls",
		Short: "List files in a volume directory",
		Usage: "ls [flags] <volume>:<path>",
		Run: func(ctx context.Context, sess Session, args []string) error {
			var long, all, human bool
			var volume, path string
			path = "/"

			for _, arg := range args {
				if strings.HasPrefix(arg, "-") {
					for _, ch := range arg[1:] {
						switch ch {
						case 'l':
							long = true
						case 'a':
							all = true
						case 'h':
							human = true
						default:
							return fmt.Errorf("ls: unknown flag -%c", ch)
						}
					}
					continue
				}
				vol, p, ok := ParseVolPath(arg)
				if !ok {
					return fmt.Errorf("ls: expected volume:/path, got %q", arg)
				}
				volume = vol
				path = p
			}
			if volume == "" {
				return fmt.Errorf("ls: missing volume:/path argument")
			}

			fsys, err := sess.FS(volume)
			if err != nil {
				return err
			}

			names, err := fsys.ReadDir(path)
			if err != nil {
				return err
			}

			out := sess.Stdout()
			var buf strings.Builder
			for _, name := range names {
				if !all && strings.HasPrefix(name, ".") {
					continue
				}
				if long {
					info, err := fsys.Lstat(path + "/" + name)
					if err != nil {
						continue
					}
					buf.WriteString(formatLong(name, info, human))
					buf.WriteByte('\n')
				} else {
					buf.WriteString(name)
					buf.WriteByte('\n')
				}
			}
			_, err = fmt.Fprint(out, buf.String())
			return err
		},
	})
}

func formatLong(name string, info fs.FileInfo, human bool) string {
	modeStr := info.Mode().String()
	size := formatSize(info.Size(), human)
	dateStr := info.ModTime().Format(time.DateOnly + " 15:04")
	return fmt.Sprintf("%s %s %s %s", modeStr, size, dateStr, name)
}

func formatSize(bytes int64, human bool) string {
	if !human {
		return fmt.Sprintf("%8d", bytes)
	}
	switch {
	case bytes >= 1<<30:
		return fmt.Sprintf("%5.1fG", float64(bytes)/float64(1<<30))
	case bytes >= 1<<20:
		return fmt.Sprintf("%5.1fM", float64(bytes)/float64(1<<20))
	case bytes >= 1<<10:
		return fmt.Sprintf("%5.1fK", float64(bytes)/float64(1<<10))
	default:
		return fmt.Sprintf("%5d", bytes)
	}
}
