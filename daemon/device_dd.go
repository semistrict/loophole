package daemon

import (
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/semistrict/loophole/storage2"
)

func (d *Daemon) handleDeviceDDWrite(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	volume := r.URL.Query().Get("volume")
	offsetStr := r.URL.Query().Get("offset")
	if volume == "" || offsetStr == "" {
		http.Error(w, "volume and offset query params required", 400)
		return
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset: "+err.Error(), 400)
		return
	}

	vm := d.backend.VM()
	vol := vm.GetVolume(volume)
	if vol == nil {
		http.Error(w, "volume not found: "+volume, 404)
		return
	}

	buf := make([]byte, storage2.BlockSize)

	// Use ReadFull to guarantee a complete block read even when HTTP
	// delivers the body in smaller chunks. Each request carries exactly
	// one block (Content-Length == BlockSize), so this enables the
	// direct-to-L2 write fast path.
	n, readErr := io.ReadFull(r.Body, buf)
	if n > 0 {
		if err := vol.Write(buf[:n], offset); err != nil {
			slog.Error("device/dd/write: write failed", "volume", volume, "offset", offset, "error", err)
			http.Error(w, "write failed: "+err.Error(), 500)
			return
		}
	}
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		slog.Error("device/dd/write: read body failed", "error", readErr)
		http.Error(w, "read body failed: "+readErr.Error(), 500)
		return
	}

	w.WriteHeader(204)
}

func (d *Daemon) handleDeviceDDRead(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	volume := r.URL.Query().Get("volume")
	offsetStr := r.URL.Query().Get("offset")
	sizeStr := r.URL.Query().Get("size")
	if volume == "" || offsetStr == "" || sizeStr == "" {
		http.Error(w, "volume, offset, and size query params required", 400)
		return
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset: "+err.Error(), 400)
		return
	}
	size, err := strconv.ParseUint(sizeStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid size: "+err.Error(), 400)
		return
	}

	vm := d.backend.VM()
	vol, err := vm.OpenVolume(volume)
	if err != nil {
		http.Error(w, "open volume: "+err.Error(), 500)
		return
	}

	buf := make([]byte, size)
	n, err := vol.Read(r.Context(), buf, offset)
	if err != nil {
		slog.Error("device/dd/read: read failed", "volume", volume, "offset", offset, "error", err)
		http.Error(w, "read failed: "+err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(n))
	w.WriteHeader(200)
	_, _ = w.Write(buf[:n])
}

func (d *Daemon) handleDeviceDDFinalize(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	volume := r.URL.Query().Get("volume")
	if volume == "" {
		http.Error(w, "volume query param required", 400)
		return
	}

	vm := d.backend.VM()
	vol := vm.GetVolume(volume)
	if vol == nil {
		http.Error(w, "volume not found: "+volume, 404)
		return
	}

	if err := vol.Flush(); err != nil {
		slog.Error("device/dd/finalize: flush failed", "volume", volume, "error", err)
		http.Error(w, "flush failed: "+err.Error(), 500)
		return
	}

	if err := vol.ReleaseRef(); err != nil {
		slog.Warn("device/dd/finalize: release ref", "volume", volume, "error", err)
	}

	slog.Info("device/dd/finalize: complete", "volume", volume)
	w.WriteHeader(204)
}
