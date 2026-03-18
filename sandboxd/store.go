//go:build linux

package sandboxd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/semistrict/loophole/env"
	"github.com/semistrict/loophole/internal/util"
)

const (
	zygotesFile   = "zygotes.json"
	sandboxesFile = "sandboxes.json"
)

func loadState(dir env.Dir) (persistedState, error) {
	state := persistedState{
		Zygotes:   map[string]ZygoteRecord{},
		Sandboxes: map[string]SandboxRecord{},
	}
	root := dir.SandboxdState()
	if err := os.MkdirAll(root, 0o755); err != nil {
		return state, err
	}
	if err := loadJSON(filepath.Join(root, zygotesFile), &state.Zygotes); err != nil {
		return state, err
	}
	if err := loadJSON(filepath.Join(root, sandboxesFile), &state.Sandboxes); err != nil {
		return state, err
	}
	return state, nil
}

func saveState(dir env.Dir, state persistedState) error {
	root := dir.SandboxdState()
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := saveJSON(filepath.Join(root, zygotesFile), state.Zygotes); err != nil {
		return err
	}
	if err := saveJSON(filepath.Join(root, sandboxesFile), state.Sandboxes); err != nil {
		return err
	}
	return nil
}

func loadJSON(path string, target any) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func saveJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		util.SafeClose(f, "close sandboxd state file after write failure")
		return err
	}
	if err := f.Sync(); err != nil {
		util.SafeClose(f, "close sandboxd state file after sync failure")
		return err
	}
	util.SafeClose(f, "close sandboxd state file")
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", path, err)
	}
	return nil
}

func sortedZygotes(m map[string]ZygoteRecord) []ZygoteRecord {
	out := make([]ZygoteRecord, 0, len(m))
	for _, z := range m {
		out = append(out, z)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sortedSandboxes(m map[string]SandboxRecord) []SandboxRecord {
	out := make([]SandboxRecord, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}
