package daemon

import (
	"fmt"
	"net/http"
	"strconv"
)

func (d *Daemon) sandboxDebugInfo() any {
	info := map[string]any{
		"configured_mode": d.inst.SandboxMode,
	}
	if debugger, ok := d.sandboxRuntime.(sandboxRuntimeDebugger); ok {
		info["runtime"] = debugger.DebugInfo()
	}
	return info
}

func (d *Daemon) handleSandboxRuntime(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, d.sandboxDebugInfo())
}

func (d *Daemon) handleSandboxLog(w http.ResponseWriter, r *http.Request) {
	reader, ok := d.sandboxRuntime.(sandboxRuntimeLogReader)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("sandbox runtime does not expose logs"))
		return
	}

	volume := r.URL.Query().Get("volume")
	if volume == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("missing volume parameter"))
		return
	}

	lines := 200
	if raw := r.URL.Query().Get("lines"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid lines parameter %q", raw))
			return
		}
		lines = n
	}

	data, err := reader.DebugLog(volume, lines)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, data)
}
