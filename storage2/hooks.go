//go:build !js

package storage2

import "sync"

// volumeHooks provides LIFO hook registration and firing for volume lifecycle
// events. Embed in volume types and call fire methods at the appropriate points.
type volumeHooks struct {
	mu           sync.Mutex
	beforeFreeze []func() error
}

// OnBeforeFreeze registers a hook called before the volume is frozen.
func (h *volumeHooks) OnBeforeFreeze(fn func() error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.beforeFreeze = append(h.beforeFreeze, fn)
}

// fireBeforeFreeze calls all beforeFreeze hooks in LIFO order.
// Returns the first error encountered.
func (h *volumeHooks) fireBeforeFreeze() error {
	h.mu.Lock()
	hooks := make([]func() error, len(h.beforeFreeze))
	copy(hooks, h.beforeFreeze)
	h.mu.Unlock()

	for i := len(hooks) - 1; i >= 0; i-- {
		if err := hooks[i](); err != nil {
			return err
		}
	}
	return nil
}
