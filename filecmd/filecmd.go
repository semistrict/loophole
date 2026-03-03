// Package filecmd defines file subcommands (cat, ls, tar) that run on the
// daemon. Each command gets a Session for accessing volumes and requesting
// files from the client.
package filecmd

import (
	"context"
	"io"

	"github.com/semistrict/loophole/fsbackend"
)

// Session is the interface that commands use to interact with the daemon
// and the client. The daemon implements this over the websocket connection.
type Session interface {
	// FS returns a filesystem for the given volume name.
	FS(volume string) (fsbackend.FS, error)

	// Read requests a file from the client and returns a streaming reader.
	// The path is sent to the client which opens and streams the file back.
	Read(path string) (io.ReadCloser, error)

	// Stdout returns a writer for standard output.
	Stdout() io.Writer

	// Stderr returns a writer for standard error.
	Stderr() io.Writer
}

// Command is a file subcommand.
type Command struct {
	Name  string
	Short string
	Usage string

	// Run executes the command on the server side. Args are the raw CLI
	// arguments (not including the command name itself).
	Run func(ctx context.Context, sess Session, args []string) error
}

// Commands is all registered file commands.
var Commands []Command

// Register adds a command to the registry.
func Register(cmd Command) {
	Commands = append(Commands, cmd)
}

// Lookup finds a command by name.
func Lookup(name string) *Command {
	for i := range Commands {
		if Commands[i].Name == name {
			return &Commands[i]
		}
	}
	return nil
}
