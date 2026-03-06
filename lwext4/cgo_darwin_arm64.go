//go:build darwin && arm64

package lwext4

// #cgo LDFLAGS: -L${SRCDIR}/../build/darwin-arm64/lwext4
import "C"
