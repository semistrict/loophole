package cached

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

// SocketPath returns the UDS path for the daemon serving dir.
// If the direct path would exceed macOS's 104-char limit, a short
// hash-based path under os.TempDir() is used instead.
func SocketPath(dir string) string {
	sock := filepath.Join(dir, "cached.sock")
	if len(sock) < 100 { // leave margin for safety
		return sock
	}
	h := sha256.Sum256([]byte(dir))
	return filepath.Join(os.TempDir(), fmt.Sprintf("lh-%x.sock", h[:8]))
}

// Op identifies a protocol message type.
type Op uint8

const (
	OpLookup   Op = 1
	OpPopulate Op = 2
	OpDelete   Op = 3
	OpDrained  Op = 4
	OpCount    Op = 5
	OpDrain    Op = 6
	OpResume   Op = 7
)

// ClientMsg is a message from client to daemon.
type ClientMsg struct {
	Op      Op     `json:"op"`
	LayerID string `json:"layer_id,omitempty"`
	PageIdx uint64 `json:"page_idx,omitempty"`
	Data    []byte `json:"data,omitempty"`
}

// DaemonMsg is a message from daemon to client.
type DaemonMsg struct {
	Op    Op     `json:"op"`
	Hit   bool   `json:"hit,omitempty"`
	Slot  int    `json:"slot"`
	Count int    `json:"count,omitempty"`
	Error string `json:"error,omitempty"`
}
