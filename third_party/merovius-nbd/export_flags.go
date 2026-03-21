package nbd

// Cross-platform NBD export flag constants.
// These duplicate the values from nbdnl (which is linux-only) so that
// TCP-based NBD servers can set flags on any platform.
const (
	FlagHasFlags        uint16 = 1 << 0
	FlagReadOnly        uint16 = 1 << 1
	FlagSendFlush       uint16 = 1 << 2
	FlagSendFUA         uint16 = 1 << 3
	FlagSendTrim        uint16 = 1 << 5
	FlagSendWriteZeroes uint16 = 1 << 6
	FlagCanMulticonn    uint16 = 1 << 8
)

// ExportFlags returns the appropriate NBD export flags for d.
// It auto-detects optional capabilities (Trimmer, WriteZeroer, FUAWriter)
// via type assertion.
func ExportFlags(d Device, readOnly bool) uint16 {
	flags := FlagHasFlags | FlagSendFlush
	if readOnly {
		flags |= FlagReadOnly
	}
	if _, ok := d.(Trimmer); ok {
		flags |= FlagSendTrim
	}
	if _, ok := d.(WriteZeroer); ok {
		flags |= FlagSendWriteZeroes
	}
	if _, ok := d.(FUAWriter); ok {
		flags |= FlagSendFUA
	}
	return flags
}
