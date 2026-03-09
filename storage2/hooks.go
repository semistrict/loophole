//go:build !js

package storage2

import (
	"log/slog"
	"sync"
)

// volumeHooks provides LIFO hook registration and firing for volume lifecycle
// events. Embed in volume types and call fire methods at the appropriate points.
type volumeHooks struct {
	mu           sync.Mutex
	beforeFreeze []func() error
	beforeClose  []func()
}

// OnBeforeFreeze registers a hook called before the volume is frozen.
func (h *volumeHooks) OnBeforeFreeze(fn func() error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.beforeFreeze = append(h.beforeFreeze, fn)
}

// OnBeforeClose registers a hook called when the last ref is released.
func (h *volumeHooks) OnBeforeClose(fn func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.beforeClose = append(h.beforeClose, fn)
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

// fireBeforeClose calls all beforeClose hooks in LIFO order.
// Errors are logged but never abort the close.
func (h *volumeHooks) fireBeforeClose() {
	h.mu.Lock()
	hooks := make([]func(), len(h.beforeClose))
	copy(hooks, h.beforeClose)
	h.mu.Unlock()

	for i := len(hooks) - 1; i >= 0; i-- {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("panic in beforeClose hook", "recover", r)
				}
			}()
			hooks[i]()
		}()
	}
}
