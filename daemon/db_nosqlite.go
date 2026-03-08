//go:build nosqlite

package daemon

import "net/http"

func (d *Daemon) registerDBRoutes(mux *http.ServeMux) {}
