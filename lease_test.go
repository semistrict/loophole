package loophole

import (
	"context"
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==========================================================================
// Lifecycle tests
// ==========================================================================

func TestLeaseAcquireCreatesFile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))

		require.NoError(t, lm.EnsureStarted(t.Context()))

		data, ok := store.GetObject("leases/" + lm.Token() + ".json")
		assert.True(t, ok, "lease file should exist")

		var lf leaseFile
		require.NoError(t, json.Unmarshal(data, &lf))
		assert.Nil(t, lf.Inbox)
		assert.Nil(t, lf.Outbox)

		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseCloseDeletesFile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))
		require.NoError(t, lm.EnsureStarted(t.Context()))
		token := lm.Token()

		require.NoError(t, lm.Close(t.Context()))

		_, ok := store.GetObject("leases/" + token + ".json")
		assert.False(t, ok, "lease file should be deleted after Close")
	})
}

func TestLeaseCloseWithoutStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))
		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseEnsureStartedIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))

		require.NoError(t, lm.EnsureStarted(t.Context()))
		require.NoError(t, lm.EnsureStarted(t.Context()))
		require.NoError(t, lm.EnsureStarted(t.Context()))

		// Only one file should exist.
		keys := store.Keys("leases/")
		assert.Len(t, keys, 1)

		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestLeaseTokenIsUnique(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		a := NewLeaseManager(leases)
		b := NewLeaseManager(leases)
		assert.NotEqual(t, a.Token(), b.Token())
	})
}

// ==========================================================================
// CheckAvailable tests
// ==========================================================================

func TestCheckAvailableEmptyToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))
		require.NoError(t, lm.EnsureStarted(t.Context()))

		assert.NoError(t, lm.CheckAvailable(t.Context(), ""))
		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestCheckAvailableOwnToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))
		require.NoError(t, lm.EnsureStarted(t.Context()))

		assert.NoError(t, lm.CheckAvailable(t.Context(), lm.Token()))
		require.NoError(t, lm.Close(t.Context()))
	})
}

func TestCheckAvailableLiveHolder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))

		challenger := NewLeaseManager(leases)
		err := challenger.CheckAvailable(t.Context(), holder.Token())
		assert.ErrorContains(t, err, "lease held by")

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestCheckAvailableAfterCleanShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))
		holderToken := holder.Token()

		require.NoError(t, holder.Close(t.Context()))

		challenger := NewLeaseManager(leases)
		assert.NoError(t, challenger.CheckAvailable(t.Context(), holderToken))
	})
}

func TestCheckAvailableAfterCrash(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))
		holderToken := holder.Token()

		// Crash: stop poll loop without deleting file.
		holder.mu.Lock()
		holder.stop()
		holder.mu.Unlock()
		<-holder.done

		// File still exists — lease is still held.
		_, ok := store.GetObject("leases/" + holderToken + ".json")
		assert.True(t, ok)

		challenger := NewLeaseManager(leases)
		err := challenger.CheckAvailable(t.Context(), holderToken)
		assert.ErrorContains(t, err, "lease held by")
	})
}

func TestCheckAvailableNonexistentToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		lm := NewLeaseManager(store.At("leases"))

		// Token that was never created — should be available.
		assert.NoError(t, lm.CheckAvailable(t.Context(), "nonexistent-token"))
	})
}

// ==========================================================================
// RPC Call tests
// ==========================================================================

func TestCallHolderResponds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		holder.Handle("ping", func(ctx context.Context, params json.RawMessage) (any, error) {
			return map[string]string{"pong": "ok"}, nil
		})
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)

		type callResult struct {
			result json.RawMessage
			err    error
		}
		done := make(chan callResult, 1)
		go func() {
			result, err := caller.Call(t.Context(), holder.Token(), "ping", nil)
			done <- callResult{result, err}
		}()

		time.Sleep(leasePollInterval + time.Second)

		r := <-done
		require.NoError(t, r.err)
		assert.JSONEq(t, `{"pong":"ok"}`, string(r.result))

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestCallWithParams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		holder.Handle("echo", func(ctx context.Context, params json.RawMessage) (any, error) {
			var req struct{ Msg string }
			if err := json.Unmarshal(params, &req); err != nil {
				return nil, err
			}
			return map[string]string{"echo": req.Msg}, nil
		})
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)

		type callResult struct {
			result json.RawMessage
			err    error
		}
		done := make(chan callResult, 1)
		go func() {
			result, err := caller.Call(t.Context(), holder.Token(), "echo", map[string]string{"msg": "hello"})
			done <- callResult{result, err}
		}()

		time.Sleep(leasePollInterval + time.Second)

		r := <-done
		require.NoError(t, r.err)
		assert.JSONEq(t, `{"echo":"hello"}`, string(r.result))

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestCallHolderDead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))
		holderToken := holder.Token()

		// Crash the holder.
		holder.mu.Lock()
		holder.stop()
		holder.mu.Unlock()
		<-holder.done

		caller := NewLeaseManager(leases)

		done := make(chan error, 1)
		go func() {
			_, err := caller.Call(t.Context(), holderToken, "ping", nil)
			done <- err
		}()

		time.Sleep(leaseCallTimeout + time.Second)

		err := <-done
		assert.ErrorContains(t, err, "did not respond")
	})
}

func TestCallFileDeletedIsSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)

		done := make(chan error, 1)
		go func() {
			_, err := caller.Call(t.Context(), holder.Token(), "release", nil)
			done <- err
		}()

		// Holder does clean Close while caller is waiting.
		time.Sleep(2 * time.Second)
		require.NoError(t, holder.Close(t.Context()))

		time.Sleep(leaseCallPollInterval + time.Second)

		assert.NoError(t, <-done)
	})
}

func TestCallNonexistentToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")
		caller := NewLeaseManager(leases)

		// No lease file exists for this token — Call should fail immediately.
		_, err := caller.Call(t.Context(), "nonexistent", "ping", nil)
		assert.Error(t, err)
	})
}

func TestCallUnknownMethod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)

		done := make(chan error, 1)
		go func() {
			_, err := caller.Call(t.Context(), holder.Token(), "bogus", nil)
			done <- err
		}()

		time.Sleep(leasePollInterval + time.Second)

		err := <-done
		assert.ErrorContains(t, err, `unknown method "bogus"`)

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestCallHandlerReturnsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		holder.Handle("fail", func(ctx context.Context, params json.RawMessage) (any, error) {
			return nil, assert.AnError
		})
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)

		done := make(chan error, 1)
		go func() {
			_, err := caller.Call(t.Context(), holder.Token(), "fail", nil)
			done <- err
		}()

		time.Sleep(leasePollInterval + time.Second)

		err := <-done
		assert.ErrorContains(t, err, "assert.AnError")

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestCallContextCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)
		ctx, cancel := context.WithCancel(t.Context())

		done := make(chan error, 1)
		go func() {
			_, err := caller.Call(ctx, holder.Token(), "ping", nil)
			done <- err
		}()

		// Cancel before the holder can respond.
		time.Sleep(2 * time.Second)
		cancel()

		err := <-done
		assert.ErrorIs(t, err, context.Canceled)

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestCallSequentialCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		var callCount int
		holder := NewLeaseManager(leases)
		holder.Handle("inc", func(ctx context.Context, params json.RawMessage) (any, error) {
			callCount++
			return map[string]int{"n": callCount}, nil
		})
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)

		// First call.
		type callResult struct {
			result json.RawMessage
			err    error
		}
		done := make(chan callResult, 1)
		go func() {
			r, err := caller.Call(t.Context(), holder.Token(), "inc", nil)
			done <- callResult{r, err}
		}()
		time.Sleep(leasePollInterval + time.Second)
		r := <-done
		require.NoError(t, r.err)
		assert.JSONEq(t, `{"n":1}`, string(r.result))

		// Second call — new request ID, should be handled separately.
		go func() {
			r, err := caller.Call(t.Context(), holder.Token(), "inc", nil)
			done <- callResult{r, err}
		}()
		time.Sleep(leasePollInterval + time.Second)
		r = <-done
		require.NoError(t, r.err)
		assert.JSONEq(t, `{"n":2}`, string(r.result))

		assert.Equal(t, 2, callCount)
		require.NoError(t, holder.Close(t.Context()))
	})
}

// ==========================================================================
// Poll loop tests
// ==========================================================================

func TestPollLoopIgnoresEmptyInbox(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		require.NoError(t, holder.EnsureStarted(t.Context()))

		time.Sleep(3 * leasePollInterval)

		lf, _, err := ReadJSON[leaseFile](t.Context(), leases, holder.Token()+".json")
		require.NoError(t, err)
		assert.Nil(t, lf.Outbox)

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestPollLoopDoesNotRehandleSameRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		var callCount int
		holder := NewLeaseManager(leases)
		holder.Handle("count", func(ctx context.Context, params json.RawMessage) (any, error) {
			callCount++
			return map[string]int{"n": callCount}, nil
		})
		require.NoError(t, holder.EnsureStarted(t.Context()))

		caller := NewLeaseManager(leases)

		done := make(chan json.RawMessage, 1)
		go func() {
			result, _ := caller.Call(t.Context(), holder.Token(), "count", nil)
			done <- result
		}()

		time.Sleep(leasePollInterval + time.Second)
		result := <-done
		assert.JSONEq(t, `{"n":1}`, string(result))

		// Wait more poll intervals — should not re-handle.
		time.Sleep(3 * leasePollInterval)
		assert.Equal(t, 1, callCount)

		require.NoError(t, holder.Close(t.Context()))
	})
}

func TestPollLoopDispatchesDirectInboxWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemStore()
		leases := store.At("leases")

		holder := NewLeaseManager(leases)
		holder.Handle("release", func(ctx context.Context, params json.RawMessage) (any, error) {
			return map[string]string{"status": "released"}, nil
		})
		require.NoError(t, holder.EnsureStarted(t.Context()))

		// Directly write a request to the inbox (simulating external caller).
		err := ModifyJSON[leaseFile](t.Context(), leases, holder.Token()+".json", func(lf *leaseFile) error {
			lf.Inbox = &leaseRPCRequest{
				ID:     "test-id-123",
				Method: "release",
			}
			return nil
		})
		require.NoError(t, err)

		time.Sleep(leasePollInterval + time.Second)

		lf, _, err := ReadJSON[leaseFile](t.Context(), leases, holder.Token()+".json")
		require.NoError(t, err)
		require.NotNil(t, lf.Outbox)
		assert.Equal(t, "test-id-123", lf.Outbox.ID)
		assert.Empty(t, lf.Outbox.Error)
		assert.JSONEq(t, `{"status":"released"}`, string(lf.Outbox.Result))

		require.NoError(t, holder.Close(t.Context()))
	})
}
