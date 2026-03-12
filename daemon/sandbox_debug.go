package daemon

import "net/http"

func (d *Daemon) sandboxDebugInfo() any {
	info := map[string]any{}
	if debugger, ok := d.sandboxRuntime.(sandboxRuntimeDebugger); ok {
		info["runtime"] = debugger.DebugInfo()
	}
	return info
}

func (d *Daemon) handleSandboxRuntime(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, d.sandboxDebugInfo())
}
