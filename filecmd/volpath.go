package filecmd

import "strings"

// ParseVolPath splits a "volume:/path" string into volume and path.
// If s doesn't contain ":" or the part before ":" contains "/" or ".",
// it is not a volume path and isVol is false.
func ParseVolPath(s string) (volume, path string, isVol bool) {
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return "", s, false
	}
	prefix := s[:i]
	if strings.ContainsAny(prefix, "/.") {
		return "", s, false
	}
	return prefix, s[i+1:], true
}
