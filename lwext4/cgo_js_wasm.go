//go:build js && wasm

package lwext4

// #cgo LDFLAGS: -L${SRCDIR}/../build/js-wasm/lwext4
import "C"
