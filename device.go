package loophole

// Mode selects the block device mechanism.
type Mode string

const (
	ModeFUSE       Mode = "fuse"
	ModeNBD        Mode = "nbd"
	ModeTestNBDTCP Mode = "testnbdtcp"
	ModeLwext4FUSE Mode = "lwext4fuse"
)

// NeedsRoot reports whether the mode requires root privileges.
func (m Mode) NeedsRoot() bool {
	switch m {
	case ModeLwext4FUSE:
		return false
	default:
		return true
	}
}
