package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// loopholeCLI wraps the loophole binary for volume operations.
type loopholeCLI struct {
	bin     string // path to loophole binary
	profile string // -p flag
}

func newLoopholeCLI() *loopholeCLI {
	bin := envOr("LOOPHOLE_BIN", "/usr/local/bin/loophole")
	profile := os.Getenv("LOOPHOLE_PROFILE")
	return &loopholeCLI{bin: bin, profile: profile}
}

func (l *loopholeCLI) args(a ...string) []string {
	if l.profile != "" {
		return append([]string{"-p", l.profile}, a...)
	}
	return a
}

func (l *loopholeCLI) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, l.bin, l.args(args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (l *loopholeCLI) listVolumes(ctx context.Context) ([]string, error) {
	out, err := l.run(ctx, "ls")
	if err != nil {
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	return strings.Split(out, "\n"), nil
}

type checkpointInfo struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at"`
}

func (l *loopholeCLI) listCheckpoints(ctx context.Context, volume string) ([]checkpointInfo, error) {
	out, err := l.run(ctx, "checkpoints", volume)
	if err != nil {
		return nil, err
	}
	if out == "" {
		return nil, nil
	}
	// Parse "ID  TIMESTAMP" lines
	var result []checkpointInfo
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 1 {
			cp := checkpointInfo{ID: fields[0]}
			if len(fields) >= 2 {
				cp.CreatedAt = fields[1]
			}
			result = append(result, cp)
		}
	}
	return result, nil
}

func (l *loopholeCLI) createVolume(ctx context.Context, params map[string]any) error {
	args := []string{"create"}
	if v, ok := params["volume"].(string); ok && v != "" {
		args = append(args, v)
	}
	if size, ok := params["size"].(float64); ok && size > 0 {
		args = append(args, "--size", fmt.Sprintf("%.0f", size))
	}
	_, err := l.run(ctx, args...)
	return err
}

func (l *loopholeCLI) deleteVolume(ctx context.Context, volume string) error {
	_, err := l.run(ctx, "delete", volume)
	return err
}
