//go:build linux

package sandboxd

import "time"

const (
	StateRunning = "running"
	StateStopped = "stopped"
	StateBroken  = "broken"
	StateMissing = "missing"
)

const (
	SourceKindZygote     = "zygote"
	SourceKindVolume     = "volume"
	SourceKindCheckpoint = "checkpoint"
	SourceKindSandbox    = "sandbox"
)

const (
	VolumeModeAttach = "attach"
	VolumeModeClone  = "clone"
)

const (
	NetworkHost = "host"
)

type SourceSpec struct {
	Kind       string `json:"kind"`
	Zygote     string `json:"zygote,omitempty"`
	Volume     string `json:"volume,omitempty"`
	Mode       string `json:"mode,omitempty"`
	Checkpoint string `json:"checkpoint,omitempty"`
	SandboxID  string `json:"sandbox_id,omitempty"`
}

type ZygoteRecord struct {
	Name       string    `json:"name"`
	Volume     string    `json:"volume,omitempty"`
	Checkpoint string    `json:"checkpoint,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type SandboxRecord struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	State        string            `json:"state"`
	Source       SourceSpec        `json:"source"`
	RootfsVolume string            `json:"rootfs_volume"`
	Mountpoint   string            `json:"mountpoint"`
	OwnerSocket  string            `json:"owner_socket"`
	OwnerMode    string            `json:"owner_mode"`
	RunscID      string            `json:"runsc_id"`
	Network      string            `json:"network"`
	Env          map[string]string `json:"env,omitempty"`
	Cwd          string            `json:"cwd,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Entrypoint   []string          `json:"entrypoint,omitempty"`
	Derived      bool              `json:"derived"`
	CreatedAt    time.Time         `json:"created_at"`
	StartedAt    *time.Time        `json:"started_at,omitempty"`
	StoppedAt    *time.Time        `json:"stopped_at,omitempty"`
}

type persistedState struct {
	Zygotes   map[string]ZygoteRecord  `json:"zygotes"`
	Sandboxes map[string]SandboxRecord `json:"sandboxes"`
}

type RegisterZygoteRequest struct {
	Name       string `json:"name"`
	Volume     string `json:"volume,omitempty"`
	Checkpoint string `json:"checkpoint,omitempty"`
}

type CreateSandboxRequest struct {
	Name       string            `json:"name,omitempty"`
	Source     SourceSpec        `json:"source"`
	Env        map[string]string `json:"env,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Network    string            `json:"network,omitempty"`
	Entrypoint []string          `json:"entrypoint,omitempty"`
}

type ProcessCreateRequest struct {
	Command    []string          `json:"command,omitempty"`
	Argv       []string          `json:"argv,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Background bool              `json:"background,omitempty"`
	TTY        bool              `json:"tty,omitempty"`
	Rows       int               `json:"rows,omitempty"`
	Cols       int               `json:"cols,omitempty"`
}

type ProcessRecord struct {
	ID         string            `json:"id"`
	SandboxID  string            `json:"sandbox_id"`
	Command    []string          `json:"command"`
	Cwd        string            `json:"cwd,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	TTY        bool              `json:"tty"`
	Background bool              `json:"background"`
	State      string            `json:"state"`
	ExitCode   *int              `json:"exit_code,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	StartedAt  *time.Time        `json:"started_at,omitempty"`
	StoppedAt  *time.Time        `json:"stopped_at,omitempty"`
}

type ExecResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}
