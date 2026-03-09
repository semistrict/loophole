package loophole

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// leasePollInterval is the default (idle) poll interval.
	leasePollInterval = 10 * time.Second

	// leasePollFast is the poll interval after receiving an RPC message.
	leasePollFast = 1 * time.Second

	// leasePollDecay is how long to stay in fast mode after the last RPC.
	leasePollDecay = 20 * time.Second

	// leaseCallTimeout is how long Call waits for a response.
	leaseCallTimeout = 30 * time.Second

	// leaseCallPollInterval is how often Call checks the outbox.
	leaseCallPollInterval = 1 * time.Second
)

// leaseRPCRequest is written to the inbox by Call.
type leaseRPCRequest struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// leaseRPCResponse is written to the outbox by the handler.
type leaseRPCResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// leaseFile is the JSON stored at leases/<token>.json — one per process.
type leaseFile struct {
	Inbox  *leaseRPCRequest  `json:"inbox,omitempty"`
	Outbox *leaseRPCResponse `json:"outbox,omitempty"`
}

// LeaseHandler handles an incoming RPC request.
type LeaseHandler func(ctx context.Context, params json.RawMessage) (any, error)

// LeaseManager manages a single process-wide lease token stored in S3.
// The lease file is created lazily on the first EnsureStarted call.
// A background goroutine GET-polls the lease file every 10s to check
// for inbox messages and dispatch them to registered handlers.
// On Close the file is deleted, signaling a clean shutdown.
//
// This design minimizes S3 costs: GET is ~12x cheaper than PUT, so
// polling with GET replaces expensive periodic PUT-based renewal.
type LeaseManager struct {
	token  string
	leases ObjectStore // base.At("leases")

	handlers map[string]LeaseHandler

	mu      sync.Mutex
	started bool
	etag    string // etag of our lease file, updated on each write
	stop    context.CancelFunc
	done    chan struct{}
}

// NewLeaseManager creates a manager with a fresh token. No S3 writes
// happen until EnsureStarted is called.
func NewLeaseManager(leases ObjectStore) *LeaseManager {
	lm := &LeaseManager{
		token:    uuid.NewString(),
		leases:   leases,
		handlers: make(map[string]LeaseHandler),
		done:     make(chan struct{}),
	}
	lm.handlers["check_alive"] = func(_ context.Context, _ json.RawMessage) (any, error) {
		return map[string]string{"status": "alive"}, nil
	}
	return lm
}

// Token returns the process-wide lease token.
func (lm *LeaseManager) Token() string { return lm.token }

// Handle registers a handler for the given RPC method.
// Must be called before EnsureStarted.
func (lm *LeaseManager) Handle(method string, fn LeaseHandler) {
	lm.handlers[method] = fn
}

// EnsureStarted creates the lease file and starts the background
// poll loop on first call. Subsequent calls are no-ops.
func (lm *LeaseManager) EnsureStarted(ctx context.Context) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.started {
		return nil
	}
	if err := lm.createLease(ctx); err != nil {
		return fmt.Errorf("create lease file: %w", err)
	}
	slog.Info("lease: started", "token", lm.token)
	pollCtx, cancel := context.WithCancel(context.Background())
	lm.stop = cancel
	lm.started = true
	go lm.pollLoop(pollCtx)
	return nil
}

// CheckAvailable returns nil if the given token's lease file is absent
// or belongs to us. If the lease file exists, sends a check_alive RPC
// and waits up to 20s. If the holder responds, returns an error (lease
// is actively held). If no response, auto-breaks the lease and returns nil.
func (lm *LeaseManager) CheckAvailable(ctx context.Context, existingToken string) error {
	if existingToken == "" || existingToken == lm.token {
		return nil
	}
	_, _, err := lm.leases.Get(ctx, existingToken+".json")
	if err != nil {
		return nil // missing → available
	}

	// Lease file exists — check if the holder is still alive.
	slog.Info("lease: checking if holder is alive", "holder", existingToken, "token", lm.token)
	checkCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, rpcErr := lm.Call(checkCtx, existingToken, "check_alive", nil)
	if rpcErr == nil {
		// Holder responded successfully — lease is actively held.
		return fmt.Errorf("lease held by %q", existingToken)
	}
	if checkCtx.Err() == nil {
		// Context is not expired, so the error is a response from the holder
		// (e.g. remote error, or file deleted). Either way, holder is reachable
		// or gone cleanly — not a stale lease.
		// File deleted → available (Call returns nil, nil for that case, handled above).
		// Remote error → holder is alive but returned an error.
		return fmt.Errorf("lease held by %q", existingToken)
	}

	// Timed out waiting for response — holder is dead.
	slog.Warn("lease: holder not responding, auto-breaking stale lease", "holder", existingToken, "token", lm.token)
	if err := lm.forceBreakLease(ctx, existingToken); err != nil {
		return fmt.Errorf("auto-break lease %q: %w", existingToken, err)
	}
	return nil
}

// forceBreakLease clears a lease token and deletes the lease file.
func (lm *LeaseManager) forceBreakLease(ctx context.Context, token string) error {
	// Delete the stale lease file (best-effort).
	_ = lm.leases.DeleteObject(ctx, token+".json")
	return nil
}

// Call sends an RPC request to another token's inbox and waits for a
// response in the outbox. Returns the result or an error on timeout.
func (lm *LeaseManager) Call(ctx context.Context, token, method string, params any) (json.RawMessage, error) {
	key := token + ".json"
	reqID := uuid.NewString()

	var rawParams json.RawMessage
	if params != nil {
		var err error
		rawParams, err = json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("marshal params: %w", err)
		}
	}

	req := &leaseRPCRequest{
		ID:     reqID,
		Method: method,
		Params: rawParams,
	}

	// CAS-write request to inbox.
	if err := ModifyJSON[leaseFile](ctx, lm.leases, key, func(lf *leaseFile) error {
		lf.Inbox = req
		lf.Outbox = nil // clear any stale response
		return nil
	}); err != nil {
		return nil, fmt.Errorf("write %s request: %w", method, err)
	}

	// Poll outbox for response.
	deadline := time.After(leaseCallTimeout)
	ticker := time.NewTicker(leaseCallPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			return nil, fmt.Errorf("lease holder %q did not respond to %q within %s", token, method, leaseCallTimeout)
		case <-ticker.C:
			lf, _, err := ReadJSON[leaseFile](ctx, lm.leases, key)
			if err != nil {
				// File deleted — holder shut down cleanly.
				return nil, nil
			}
			if lf.Outbox == nil || lf.Outbox.ID != reqID {
				continue
			}
			if lf.Outbox.Error != "" {
				return nil, fmt.Errorf("remote: %s", lf.Outbox.Error)
			}
			return lf.Outbox.Result, nil
		}
	}
}

// Close stops the background poller and deletes the lease file.
func (lm *LeaseManager) Close(ctx context.Context) error {
	lm.mu.Lock()
	started := lm.started
	if lm.stop != nil {
		lm.stop()
	}
	lm.mu.Unlock()

	if started {
		<-lm.done
		return lm.leases.DeleteObject(ctx, lm.token+".json")
	}
	return nil
}

// --- internal ---

func (lm *LeaseManager) createLease(ctx context.Context) error {
	data, _ := json.Marshal(leaseFile{})
	if err := lm.leases.PutIfNotExists(ctx, lm.token+".json", data); err != nil {
		if errors.Is(err, ErrExists) {
			return fmt.Errorf("lease token %s already exists", lm.token)
		}
		return err
	}
	// Read back to get the etag for subsequent CAS writes.
	body, etag, err := lm.leases.Get(ctx, lm.token+".json")
	if err != nil {
		return fmt.Errorf("read back lease etag: %w", err)
	}
	if err := body.Close(); err != nil {
		slog.Warn("close failed", "error", err)
	}
	lm.etag = etag
	return nil
}

// pollLoop GET-polls the lease file to check for inbox messages.
// After handling an RPC, poll frequency increases to 1s for 20s,
// then decays back to the default 10s interval.
func (lm *LeaseManager) pollLoop(ctx context.Context) {
	defer close(lm.done)
	ticker := time.NewTicker(leasePollInterval)
	defer ticker.Stop()
	var lastRPC time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			handled := lm.checkInbox(ctx)
			if handled {
				lastRPC = time.Now()
				ticker.Reset(leasePollFast)
			} else if !lastRPC.IsZero() && time.Since(lastRPC) > leasePollDecay {
				lastRPC = time.Time{}
				ticker.Reset(leasePollInterval)
			}
		}
	}
}

// checkInbox reads the lease file and dispatches any inbox message.
// Returns true if a new RPC was handled.
func (lm *LeaseManager) checkInbox(ctx context.Context) bool {
	lf, _, err := ReadJSON[leaseFile](ctx, lm.leases, lm.token+".json")
	if err != nil {
		return false
	}
	if lf.Inbox == nil {
		return false
	}
	// Skip if we already responded to this request.
	if lf.Outbox != nil && lf.Outbox.ID == lf.Inbox.ID {
		return false
	}

	req := lf.Inbox
	slog.Info("lease: received RPC", "method", req.Method, "id", req.ID, "token", lm.token)

	handler, ok := lm.handlers[req.Method]
	resp := &leaseRPCResponse{ID: req.ID}

	if !ok {
		resp.Error = fmt.Sprintf("unknown method %q", req.Method)
	} else {
		result, err := handler(ctx, req.Params)
		if err != nil {
			resp.Error = err.Error()
		} else if result != nil {
			resp.Result, _ = json.Marshal(result)
		}
	}

	// Write response to outbox.
	_ = ModifyJSON[leaseFile](ctx, lm.leases, lm.token+".json", func(lf *leaseFile) error {
		lf.Outbox = resp
		return nil
	})
	return true
}
