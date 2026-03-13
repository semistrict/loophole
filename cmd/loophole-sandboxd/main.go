package main

import (
	"errors"
	"fmt"
	"os"
)

type exitCoder interface {
	ExitCode() int
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		var coded exitCoder
		if errors.As(err, &coded) {
			os.Exit(coded.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
