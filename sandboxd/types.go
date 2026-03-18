//go:build linux

package sandboxd

import "github.com/semistrict/loophole/sandboxapi"

const (
	StateRunning = sandboxapi.StateRunning
	StateStopped = sandboxapi.StateStopped
	StateBroken  = sandboxapi.StateBroken
	StateMissing = sandboxapi.StateMissing
)

const (
	SourceKindZygote     = sandboxapi.SourceKindZygote
	SourceKindVolume     = sandboxapi.SourceKindVolume
	SourceKindCheckpoint = sandboxapi.SourceKindCheckpoint
	SourceKindSandbox    = sandboxapi.SourceKindSandbox
)

const (
	VolumeModeAttach = sandboxapi.VolumeModeAttach
	VolumeModeClone  = sandboxapi.VolumeModeClone
)

const (
	NetworkHost = sandboxapi.NetworkHost
)

type SourceSpec = sandboxapi.SourceSpec
type ZygoteRecord = sandboxapi.ZygoteRecord
type SandboxRecord = sandboxapi.SandboxRecord
type RegisterZygoteRequest = sandboxapi.RegisterZygoteRequest
type CreateSandboxRequest = sandboxapi.CreateSandboxRequest
type ProcessCreateRequest = sandboxapi.ProcessCreateRequest
type ProcessRecord = sandboxapi.ProcessRecord
type ExecResult = sandboxapi.ExecResult
type SandboxDebugInfo = sandboxapi.SandboxDebugInfo
type ErrorResponse = sandboxapi.ErrorResponse
type StatusResponse = sandboxapi.StatusResponse

type persistedState struct {
	Zygotes   map[string]ZygoteRecord  `json:"zygotes"`
	Sandboxes map[string]SandboxRecord `json:"sandboxes"`
}
