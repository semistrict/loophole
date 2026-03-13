//go:build !linux

package main

import "fmt"

func run(_ []string) error {
	return fmt.Errorf("loophole-sandboxd is only supported on Linux")
}
