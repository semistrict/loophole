package daemon

import "net/http"

func (d *Daemon) sandboxDebugInfo() any {
	return map[string]any{
		"runtime": map[string]any{
			"type": "chroot",
		},
	}
}

func (d *Daemon) handleSandboxRuntime(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, d.sandboxDebugInfo())
}
