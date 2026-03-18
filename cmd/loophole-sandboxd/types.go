//go:build linux

package main

import "github.com/semistrict/loophole/sandboxapi"

const (
	stateRunning = sandboxapi.StateRunning
	stateStopped = sandboxapi.StateStopped
	stateBroken  = sandboxapi.StateBroken
	stateMissing = sandboxapi.StateMissing
)

const (
	sourceKindZygote     = sandboxapi.SourceKindZygote
	sourceKindVolume     = sandboxapi.SourceKindVolume
	sourceKindCheckpoint = sandboxapi.SourceKindCheckpoint
	sourceKindSandbox    = sandboxapi.SourceKindSandbox
)

const (
	volumeModeAttach = sandboxapi.VolumeModeAttach
	volumeModeClone  = sandboxapi.VolumeModeClone
)

const (
	networkHost = sandboxapi.NetworkHost
)

type sourceSpec = sandboxapi.SourceSpec
type zygoteRecord = sandboxapi.ZygoteRecord
type sandboxRecord = sandboxapi.SandboxRecord
type registerZygoteRequest = sandboxapi.RegisterZygoteRequest
type createSandboxRequest = sandboxapi.CreateSandboxRequest
type processCreateRequest = sandboxapi.ProcessCreateRequest
type processRecord = sandboxapi.ProcessRecord
type execResult = sandboxapi.ExecResult
type sandboxDebugInfo = sandboxapi.SandboxDebugInfo
type errorResponse = sandboxapi.ErrorResponse
type statusResponse = sandboxapi.StatusResponse

type persistedState struct {
	Zygotes   map[string]zygoteRecord  `json:"zygotes"`
	Sandboxes map[string]sandboxRecord `json:"sandboxes"`
}
