package loophole

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"
)

var (
	selfHashOnce sync.Once
	selfHash     string
)

// SelfHash returns the first 16 hex chars of the SHA-256 of the running binary.
// Returns "" if the binary cannot be read (e.g. deleted after startup).
func SelfHash() string {
	selfHashOnce.Do(func() {
		exe, err := os.Executable()
		if err != nil {
			return
		}
		data, err := os.ReadFile(exe)
		if err != nil {
			return
		}
		sum := sha256.Sum256(data)
		selfHash = hex.EncodeToString(sum[:8])
	})
	return selfHash
}
