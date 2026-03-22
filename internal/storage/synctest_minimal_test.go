package storage

import (
	"log/slog"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

func TestSynctestCloseChannel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			select {
			case <-stop:
				return
			case <-timer.C:
			}
		}()
		close(stop)
		<-done
		t.Log("ok")
	})
}

func TestSlogInSynctest(t *testing.T) {
	// Test that slog output is visible under synctest.
	synctest.Test(t, func(t *testing.T) {
		slog.Info("slog from synctest bubble")
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
		slog.Info("slog after SetDefault in synctest bubble")
		t.Log("t.Log from synctest bubble")
	})
}
