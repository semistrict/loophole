package env

import (
	"os"
	"strings"
)

// HasOption reports whether LOOPHOLE_OPTIONS contains the named option.
// Options may be separated by commas, whitespace, or semicolons.
func HasOption(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	for _, part := range strings.FieldsFunc(os.Getenv("LOOPHOLE_OPTIONS"), func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
	}) {
		if strings.ToLower(strings.TrimSpace(part)) == name {
			return true
		}
	}
	return false
}
