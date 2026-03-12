package daemon

import "context"

// ExecResult is the JSON response returned by /sandbox/exec.
type ExecResult struct {
	ExitCode int    `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

// SandboxRuntime executes sandboxed commands for /sandbox/exec.
type SandboxRuntime interface {
	Exec(ctx context.Context, volume string, cmd string) (ExecResult, error)
}

type sandboxRuntimeCloser interface {
	Close(ctx context.Context) error
}

type sandboxRuntimeDebugger interface {
	DebugInfo() any
}
