package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		{"5GB", 5 * 1024 * 1024 * 1024},
		{"5gb", 5 * 1024 * 1024 * 1024},
		{"100GB", 100 * 1024 * 1024 * 1024},
		{"1TB", 1024 * 1024 * 1024 * 1024},
		{"512MB", 512 * 1024 * 1024},
		{"64KB", 64 * 1024},
		{"4096B", 4096},
		{"4096", 4096},
		{"0", 0},
		{"", 0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSize(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseSizeErrors(t *testing.T) {
	tests := []string{
		"abc",
		"5XB",
		"-1GB",
		"GB",
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := parseSize(input)
			require.Error(t, err)
		})
	}
}
