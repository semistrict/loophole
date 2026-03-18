//go:build linux

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusEndpointReportsRunscPlatform(t *testing.T) {
	t.Parallel()

	d := &daemon{
		runscBin:   "/usr/local/bin/runsc",
		runscDebug: true,
		runscRoot:  "/tmp/runsc-root",
		zygotes: map[string]zygoteRecord{
			"zygote-1": {Name: "zygote-1"},
		},
		sandboxes: map[string]sandboxRecord{
			"sb-1": {ID: "sb-1"},
			"sb-2": {ID: "sb-2"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	rec := httptest.NewRecorder()
	d.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
	}

	var got statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if !got.OK {
		t.Fatalf("expected ok=true, got false")
	}
	if got.RunscPlatform != defaultRunscPlatform {
		t.Fatalf("runsc_platform = %q, want %q", got.RunscPlatform, defaultRunscPlatform)
	}
	if got.RunscPlatformSource != "bundled runsc default" {
		t.Fatalf("runsc_platform_source = %q", got.RunscPlatformSource)
	}
	if got.RunscBin != d.runscBin {
		t.Fatalf("runsc_bin = %q, want %q", got.RunscBin, d.runscBin)
	}
	if got.SandboxCount != 2 {
		t.Fatalf("sandbox_count = %d, want 2", got.SandboxCount)
	}
	if got.ZygoteCount != 1 {
		t.Fatalf("zygote_count = %d, want 1", got.ZygoteCount)
	}
}

func TestWriteErrorIncludesSandboxDebugInfo(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	writeError(rec, http.StatusInternalServerError, &sandboxDebugError{
		err: assertError("runsc run failed"),
		debug: sandboxDebugInfo{
			SandboxID:     "sbx_test",
			SandboxDir:    "/root/.loophole/sandboxd/sandboxes/sbx_test",
			RunscRunLog:   "/root/.loophole/sandboxd/sandboxes/sbx_test/runsc-run.log",
			RunscPanicLog: "/root/.loophole/sandboxd/sandboxes/sbx_test/runsc-panic.log",
			RunscDebugDir: "/root/.loophole/sandboxd/sandboxes/sbx_test/runsc-debug",
		},
	})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	var got errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Error != "runsc run failed" {
		t.Fatalf("error = %q", got.Error)
	}
	if got.Debug == nil {
		t.Fatal("expected debug payload")
	}
	if got.Debug.SandboxID != "sbx_test" {
		t.Fatalf("sandbox_id = %q", got.Debug.SandboxID)
	}
	if got.Debug.RunscRunLog == "" {
		t.Fatal("expected runsc_run_log")
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }
