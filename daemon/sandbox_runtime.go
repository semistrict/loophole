package daemon

import "context"

// ExecResult is the JSON response returned by /sandbox/exec.
type ExecResult struct {
	ExitCode int    `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

// VMSnapshot holds the metadata returned by SnapshotVM.
type VMSnapshot struct {
	SnapshotPath   string `json:"snapshot_path"`
	MemCloneVolume string `json:"mem_clone_volume"`
	SourceVolume   string `json:"source_volume"`
}

// VMCloneResult is returned by RestoreVM after a snapshot is loaded into a new VM.
type VMCloneResult struct {
	Volume   string `json:"volume"`
	GuestCID uint32 `json:"guest_cid"`
	Netns    string `json:"netns"`
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

type sandboxRuntimeLogReader interface {
	DebugLog(volume string, lines int) (map[string]any, error)
}
