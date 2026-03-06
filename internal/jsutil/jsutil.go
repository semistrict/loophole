//go:build js

// Package jsutil provides Go↔JS interop utilities for TinyGo WASM.
package jsutil

import (
	"fmt"
	"syscall/js"
)

// GoBytes copies a JS Uint8Array into a Go []byte.
func GoBytes(v js.Value) []byte {
	length := v.Get("length").Int()
	buf := make([]byte, length)
	js.CopyBytesToGo(buf, v)
	return buf
}

// JSBytes creates a JS Uint8Array from a Go []byte.
func JSBytes(data []byte) js.Value {
	arr := js.Global().Get("Uint8Array").New(len(data))
	js.CopyBytesToJS(arr, data)
	return arr
}

// Async wraps a Go function as a JS function that returns a Promise.
// The function f runs in a new goroutine. It receives the JS args and
// returns either (result, nil) or (_, error).
func Async(f func(args []js.Value) (any, error)) js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		promise := js.Global().Get("Promise")
		return promise.New(js.FuncOf(func(_ js.Value, pargs []js.Value) any {
			resolve := pargs[0]
			reject := pargs[1]
			go func() {
				result, err := f(args)
				if err != nil {
					reject.Invoke(js.Global().Get("Error").New(err.Error()))
					return
				}
				if result == nil {
					resolve.Invoke(js.Undefined())
				} else {
					resolve.Invoke(result)
				}
			}()
			return nil
		}))
	})
}

// MustString extracts a string from args[i], or returns "" if out of bounds / undefined.
func MustString(args []js.Value, i int) string {
	if i >= len(args) || args[i].IsUndefined() || args[i].IsNull() {
		return ""
	}
	return args[i].String()
}

// MustInt extracts an int from args[i], or returns 0 if out of bounds / undefined.
func MustInt(args []js.Value, i int) int {
	if i >= len(args) || args[i].IsUndefined() || args[i].IsNull() {
		return 0
	}
	return args[i].Int()
}

// MustBytes extracts a Uint8Array from args[i] as []byte.
func MustBytes(args []js.Value, i int) []byte {
	if i >= len(args) || args[i].IsUndefined() || args[i].IsNull() {
		return nil
	}
	return GoBytes(args[i])
}

// Await blocks until a JS Promise resolves or rejects.
// Returns the resolved value or an error.
func Await(promise js.Value) (js.Value, error) {
	ch := make(chan js.Value, 1)
	errCh := make(chan error, 1)

	thenFunc := js.FuncOf(func(_ js.Value, args []js.Value) any {
		ch <- args[0]
		return nil
	})
	catchFunc := js.FuncOf(func(_ js.Value, args []js.Value) any {
		errCh <- fmt.Errorf("%s", args[0].Get("message").String())
		return nil
	})
	defer thenFunc.Release()
	defer catchFunc.Release()

	promise.Call("then", thenFunc).Call("catch", catchFunc)

	select {
	case val := <-ch:
		return val, nil
	case err := <-errCh:
		return js.Undefined(), err
	}
}
