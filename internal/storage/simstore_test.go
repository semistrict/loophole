package storage

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole/internal/blob"
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

// SimObjectStore wraps a MemDriver with deterministic fault injection.
// All randomness comes from the provided *rand.Rand for reproducibility.
type SimObjectStore struct {
	inner  *blob.MemDriver
	store  *blob.Store
	rng    *rand.Rand
	faults FaultConfig
}

// NewSimObjectStore creates a fault-injecting object store wrapper.
func NewSimObjectStore(rng *rand.Rand, faults FaultConfig) *SimObjectStore {
	mem, store := blob.NewMemStore()
	return &SimObjectStore{
		inner:  mem,
		store:  store,
		rng:    rng,
		faults: faults,
	}
}

// Store returns a *blob.Store wrapping the inner MemDriver.
func (s *SimObjectStore) Store() *blob.Store {
	return s.store
}

// InjectFaults randomly arms faults on the underlying MemDriver for the
// current tick, based on FaultConfig probabilities. Call once per simulation tick.
func (s *SimObjectStore) InjectFaults() {
	s.inner.ClearAllFaults()

	if s.faults.GetFailRate > 0 && s.rng.Float64() < s.faults.GetFailRate {
		setOneShotFault(s.inner, blob.OpGet, "", blob.Fault{
			Err: fmt.Errorf("simulated S3 GET failure"),
		})
	}

	if s.faults.PutFailRate > 0 && s.rng.Float64() < s.faults.PutFailRate {
		setOneShotFault(s.inner, blob.OpPut, "", blob.Fault{
			Err: fmt.Errorf("simulated S3 PUT failure"),
		})
		setOneShotFault(s.inner, blob.OpPut, "", blob.Fault{
			Err: fmt.Errorf("simulated S3 PUT failure"),
		})
	}

	if s.faults.PhantomPutRate > 0 && s.rng.Float64() < s.faults.PhantomPutRate {
		setOneShotFault(s.inner, blob.OpPut, "", blob.Fault{
			PostErr: fmt.Errorf("simulated phantom PUT: data written but error returned"),
		})
		setOneShotFault(s.inner, blob.OpPut, "", blob.Fault{
			PostErr: fmt.Errorf("simulated phantom PUT: data written but error returned"),
		})
	}

	if s.faults.LayersJsonPhantomPutRate > 0 && s.rng.Float64() < s.faults.LayersJsonPhantomPutRate {
		isLayersJSON := func(key string) bool {
			return strings.HasSuffix(key, "layers.json")
		}
		setOneShotFault(s.inner, blob.OpPut, "", blob.Fault{
			PostErr:     fmt.Errorf("simulated phantom PUT on layers.json"),
			ShouldFault: isLayersJSON,
		})
		setOneShotFault(s.inner, blob.OpPut, "", blob.Fault{
			PostErr:     fmt.Errorf("simulated phantom PUT on layers.json"),
			ShouldFault: isLayersJSON,
		})
	}

	if s.faults.ListFailRate > 0 && s.rng.Float64() < s.faults.ListFailRate {
		setOneShotFault(s.inner, blob.OpList, "", blob.Fault{
			Err: fmt.Errorf("simulated S3 LIST failure"),
		})
	}

	if s.faults.DeleteFailRate > 0 && s.rng.Float64() < s.faults.DeleteFailRate {
		setOneShotFault(s.inner, blob.OpDelete, "", blob.Fault{
			Err: fmt.Errorf("simulated S3 DELETE failure"),
		})
	}

	if s.faults.GetLatency > 0 {
		s.inner.SetFault(blob.OpGet, "", blob.Fault{
			Delay: s.faults.GetLatency,
		})
	}

	if s.faults.PutLatency > 0 {
		s.inner.SetFault(blob.OpPut, "", blob.Fault{
			Delay: s.faults.PutLatency,
		})
	}
}

// ClearFaults removes all armed faults.
func (s *SimObjectStore) ClearFaults() {
	s.inner.ClearAllFaults()
}

func setOneShotFault(store *blob.MemDriver, op blob.OpType, key string, fault blob.Fault) {
	var fired atomic.Bool
	originalShouldFault := fault.ShouldFault
	fault.ShouldFault = func(fullKey string) bool {
		if originalShouldFault != nil && !originalShouldFault(fullKey) {
			return false
		}
		return !fired.Load()
	}
	fault.Hook = func() {
		if fired.CompareAndSwap(false, true) {
			store.ClearFault(op, key)
		}
	}
	store.SetFault(op, key, fault)
}
