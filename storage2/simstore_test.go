package storage2

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/semistrict/loophole"
)

// FaultConfig controls probabilistic fault injection in SimObjectStore.
type FaultConfig struct {
	// Per-operation failure probability (0.0 - 1.0).
	PutFailRate    float64
	GetFailRate    float64
	ListFailRate   float64
	DeleteFailRate float64

	// Per-operation latency (advanced via synctest's fake clock).
	PutLatency time.Duration
	GetLatency time.Duration

	// Simulate partial writes: Put succeeds in S3 but returns error to caller.
	// The object exists but the caller thinks it failed.
	PhantomPutRate float64

	// Targeted phantom PUT for layers.json only. Exercises the critical path
	// where the manifest write "fails" but data actually persisted — on next
	// open the volume sees different layers than the caller expected.
	LayersJsonPhantomPutRate float64
}

// SimObjectStore wraps a MemStore with deterministic fault injection.
// All randomness comes from the provided *rand.Rand for reproducibility.
type SimObjectStore struct {
	inner  *loophole.MemStore
	rng    *rand.Rand
	faults FaultConfig
}

// NewSimObjectStore creates a fault-injecting object store wrapper.
func NewSimObjectStore(rng *rand.Rand, faults FaultConfig) *SimObjectStore {
	return &SimObjectStore{
		inner:  loophole.NewMemStore(),
		rng:    rng,
		faults: faults,
	}
}

// Store returns the underlying MemStore (for use as loophole.ObjectStore).
// Fault injection is applied via MemStore's SetFault mechanism, driven
// by the simulation's tick cycle rather than per-call randomness, because
// MemStore.SetFault is the established pattern for this codebase.
//
// The simulation calls InjectFaults() before each tick to randomly arm
// faults based on the FaultConfig probabilities.
func (s *SimObjectStore) Store() *loophole.MemStore {
	return s.inner
}

// InjectFaults randomly arms faults on the underlying MemStore for the
// current tick, based on FaultConfig probabilities. Call once per simulation tick.
func (s *SimObjectStore) InjectFaults() {
	s.inner.ClearAllFaults()

	if s.faults.GetFailRate > 0 && s.rng.Float64() < s.faults.GetFailRate {
		s.inner.SetFault(loophole.OpGet, "", loophole.Fault{
			Err: fmt.Errorf("simulated S3 GET failure"),
		})
	}

	if s.faults.PutFailRate > 0 && s.rng.Float64() < s.faults.PutFailRate {
		s.inner.SetFault(loophole.OpPutReader, "", loophole.Fault{
			Err: fmt.Errorf("simulated S3 PUT failure"),
		})
		s.inner.SetFault(loophole.OpPutIfNotExists, "", loophole.Fault{
			Err: fmt.Errorf("simulated S3 PUT failure"),
		})
	}

	if s.faults.PhantomPutRate > 0 && s.rng.Float64() < s.faults.PhantomPutRate {
		s.inner.SetFault(loophole.OpPutReader, "", loophole.Fault{
			PostErr: fmt.Errorf("simulated phantom PUT: data written but error returned"),
		})
		s.inner.SetFault(loophole.OpPutIfNotExists, "", loophole.Fault{
			PostErr: fmt.Errorf("simulated phantom PUT: data written but error returned"),
		})
	}

	if s.faults.LayersJsonPhantomPutRate > 0 && s.rng.Float64() < s.faults.LayersJsonPhantomPutRate {
		isLayersJSON := func(key string) bool {
			return strings.HasSuffix(key, "layers.json")
		}
		s.inner.SetFault(loophole.OpPutReader, "", loophole.Fault{
			PostErr:     fmt.Errorf("simulated phantom PUT on layers.json"),
			ShouldFault: isLayersJSON,
		})
		s.inner.SetFault(loophole.OpPutIfNotExists, "", loophole.Fault{
			PostErr:     fmt.Errorf("simulated phantom PUT on layers.json"),
			ShouldFault: isLayersJSON,
		})
	}

	if s.faults.ListFailRate > 0 && s.rng.Float64() < s.faults.ListFailRate {
		s.inner.SetFault(loophole.OpListKeys, "", loophole.Fault{
			Err: fmt.Errorf("simulated S3 LIST failure"),
		})
	}

	if s.faults.DeleteFailRate > 0 && s.rng.Float64() < s.faults.DeleteFailRate {
		s.inner.SetFault(loophole.OpDeleteObject, "", loophole.Fault{
			Err: fmt.Errorf("simulated S3 DELETE failure"),
		})
	}

	if s.faults.GetLatency > 0 {
		s.inner.SetFault(loophole.OpGet, "", loophole.Fault{
			Delay: s.faults.GetLatency,
		})
	}

	if s.faults.PutLatency > 0 {
		s.inner.SetFault(loophole.OpPutReader, "", loophole.Fault{
			Delay: s.faults.PutLatency,
		})
	}
}

// ClearFaults removes all armed faults.
func (s *SimObjectStore) ClearFaults() {
	s.inner.ClearAllFaults()
}
