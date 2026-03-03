package loophole

// Mode selects the block device mechanism.
type Mode string

const (
	ModeFUSE       Mode = "fuse"
	ModeNBD        Mode = "nbd"
	ModeTestNBDTCP Mode = "testnbdtcp"
	ModeInProcess  Mode = "inprocess"
	ModeLwext4FUSE Mode = "lwext4fuse"
)

// NeedsRoot reports whether the mode requires root privileges.
func (m Mode) NeedsRoot() bool {
	switch m {
	case ModeInProcess, ModeLwext4FUSE:
		return false
	default:
		return true
	}
}
