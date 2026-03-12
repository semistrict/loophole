package storage2

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// TestSynctestOverheadScalesWithGoroutines proves that synctest's time.Sleep
// cost grows linearly with the number of timer-blocked goroutines in the
// bubble. This is the root cause of slow simulation runs with large configs:
// each volume's periodicFlushLoop adds a goroutine with a 5ms timer, and
// every tick's time.Sleep must wake/reschedule all of them.
func TestSynctestOverheadScalesWithGoroutines(t *testing.T) {
	const ticks = 500
	const timerInterval = 5 * time.Millisecond

	// measure runs ticks Sleep calls inside a synctest bubble with
	// numGoroutines timer-blocked goroutines and returns wall-clock duration.
	// Wall time is measured outside the bubble so it reflects real CPU cost,
	// not the fake clock.
	measure := func(t *testing.T, numGoroutines int) time.Duration {
		wallStart := time.Now() // real clock — outside synctest bubble
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())

			// Spawn N goroutines, each blocked on a repeating timer —
			// exactly like periodicFlushLoop.
			for range numGoroutines {
				go func() {
					timer := time.NewTimer(timerInterval)
					defer timer.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-timer.C:
							timer.Reset(timerInterval)
						}
					}
				}()
			}

			for range ticks {
				time.Sleep(timerInterval)
			}

			cancel()
		})
		return time.Since(wallStart)
	}

	counts := []int{1, 10, 50, 100, 200, 500, 1000}
	results := make([]time.Duration, len(counts))
	for i, n := range counts {
		results[i] = measure(t, n)
		if debugCountersEnabled() {
			t.Logf("goroutines=%4d  %d ticks in %v  (%.1f µs/tick)",
				n, ticks, results[i], float64(results[i].Microseconds())/ticks)
		}
	}

	// The cost at 1000 goroutines should be significantly higher than at 1.
	ratio := float64(results[len(results)-1]) / float64(results[0])
	if debugCountersEnabled() {
		t.Logf("1000 vs 1 goroutine ratio: %.1fx", ratio)
	}
	if ratio < 5 {
		t.Fatalf("expected significant slowdown with more goroutines, got only %.1fx", ratio)
	}
}
