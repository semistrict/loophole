//go:build linux && amd64

package lwext4

// #cgo LDFLAGS: -L${SRCDIR}/../build/linux-amd64/lwext4
import "C"
