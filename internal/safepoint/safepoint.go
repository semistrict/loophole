// Package safepoint protects zero-copy references (mmap slices, arena slices)
// from being invalidated while readers hold them.
//
// Readers call Enter to get a Guard, register zero-copy slices with
// Guard.Register, and call Guard.Exit when done. Exclusive operations
// (page cache drain, dirty pages cleanup) call Do to block until all
// guards have exited, then run a function under exclusive access.
package safepoint

import "sync"

// Safepoint is a readers-writer lock for zero-copy memory lifetime.
type Safepoint struct {
	mu sync.RWMutex
}

// New creates a new Safepoint.
func New() *Safepoint { return &Safepoint{} }

// Do runs f under exclusive access, blocking until all guards have exited.
func (s *Safepoint) Do(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
}

// Lock acquires exclusive access, blocking until all guards have exited.
// Must be paired with Unlock. Use Do when the critical section is a
// single function call; use Lock/Unlock when exclusive access must span
// multiple operations (e.g. page cache drain through resume).
func (s *Safepoint) Lock()   { s.mu.Lock() }
func (s *Safepoint) Unlock() { s.mu.Unlock() }

// Enter starts a zero-copy read and returns a Guard.
func (s *Safepoint) Enter() Guard { s.mu.RLock(); return Guard{s} }

// Guard represents an active zero-copy read. Slices registered with
// Register must not be used after Exit is called.
type Guard struct {
	s *Safepoint
}

// Register records slices that are being read under this guard.
// Currently a no-op; will be used for debugging.
func (g Guard) Register(o ...any) {}

// Exit releases the guard.
func (g Guard) Exit() { g.s.mu.RUnlock() }
