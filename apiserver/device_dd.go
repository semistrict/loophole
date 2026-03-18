package apiserver

import (
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/semistrict/loophole/storage"
)

func (d *Server) handleDeviceDDWrite(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	vol := d.volume()
	offsetStr := r.URL.Query().Get("offset")
	if vol == nil || offsetStr == "" {
		http.Error(w, "no volume or offset missing", 400)
		return
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset: "+err.Error(), 400)
		return
	}

	buf := make([]byte, storage.BlockSize)
	n, readErr := io.ReadFull(r.Body, buf)
	if n > 0 {
		if err := vol.Write(buf[:n], offset); err != nil {
			slog.Error("device/dd/write: write failed", "volume", vol.Name(), "offset", offset, "error", err)
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

func (d *Server) handleDeviceDDRead(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	vol := d.volume()
	offsetStr := r.URL.Query().Get("offset")
	sizeStr := r.URL.Query().Get("size")
	if vol == nil || offsetStr == "" || sizeStr == "" {
		http.Error(w, "no volume, offset, or size missing", 400)
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

	const maxReadSize = storage.BlockSize
	if size > maxReadSize {
		http.Error(w, "size exceeds maximum ("+strconv.FormatUint(maxReadSize, 10)+")", 400)
		return
	}

	buf := make([]byte, size)
	n, err := vol.Read(r.Context(), buf, offset)
	if err != nil {
		slog.Error("device/dd/read: read failed", "volume", vol.Name(), "offset", offset, "error", err)
		http.Error(w, "read failed: "+err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(n))
	w.WriteHeader(200)
	_, _ = w.Write(buf[:n])
}

func (d *Server) handleDeviceDDFinalize(w http.ResponseWriter, r *http.Request) {
	if d.rejectIfShuttingDown(w) || d.requireBackend(w) {
		return
	}

	vol := d.volume()
	if vol == nil {
		http.Error(w, "no volume managed", 400)
		return
	}

	if err := vol.Flush(); err != nil {
		slog.Error("device/dd/finalize: flush failed", "volume", vol.Name(), "error", err)
		http.Error(w, "flush failed: "+err.Error(), 500)
		return
	}

	if err := vol.ReleaseRef(); err != nil {
		slog.Warn("device/dd/finalize: release ref", "volume", vol.Name(), "error", err)
	}

	slog.Info("device/dd/finalize: complete", "volume", vol.Name())
	w.WriteHeader(204)
}
