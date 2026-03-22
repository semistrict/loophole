package env

import (
	"os"
	"strconv"
	"strings"
)

// splitOptions parses LOOPHOLE_OPTIONS into individual tokens.
// Options are separated by commas, whitespace, or semicolons.
func splitOptions() []string {
	return strings.FieldsFunc(os.Getenv("LOOPHOLE_OPTIONS"), func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
	})
}

// HasOption reports whether LOOPHOLE_OPTIONS contains the named boolean option.
// Options may be separated by commas, whitespace, or semicolons.
func HasOption(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	for _, part := range splitOptions() {
		if strings.ToLower(strings.TrimSpace(part)) == name {
			return true
		}
	}
	return false
}

// OptionInt returns the integer value of a key=value option from LOOPHOLE_OPTIONS.
// Returns fallback if the key is not set or the value is not a valid integer.
//
//	LOOPHOLE_OPTIONS="storage.maxFlushWorkers=64" → OptionInt("storage.maxFlushWorkers", 8) = 64
func OptionInt(key string, fallback int) int {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, part := range splitOptions() {
		k, v, ok := strings.Cut(part, "=")
		if ok && strings.ToLower(strings.TrimSpace(k)) == key {
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				return n
			}
		}
	}
	return fallback
}
