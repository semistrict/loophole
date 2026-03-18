package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/semistrict/loophole/internal/util"
)

func (s *controlServer) maybeEnsureDefaultZygote(ctx context.Context, source map[string]any) error {
	if len(source) == 0 {
		return nil
	}
	defaultZygote := strings.TrimSpace(os.Getenv("LOOPHOLE_DEFAULT_ZYGOTE"))
	if defaultZygote == "" {
		return nil
	}
	kind, _ := source["kind"].(string)
	zygote, _ := source["zygote"].(string)
	if kind != "zygote" || strings.TrimSpace(zygote) != defaultZygote {
		return nil
	}
	return s.ensureDefaultZygote(ctx, defaultZygote)
}

func (s *controlServer) ensureDefaultZygote(ctx context.Context, name string) error {
	s.zygoteMu.Lock()
	defer s.zygoteMu.Unlock()

	ok, err := s.hasRegisteredZygote(ctx, name)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	volume := envOr("LOOPHOLE_DEFAULT_ZYGOTE_VOLUME", "zygote-"+name)
	checkpoint, err := s.latestCheckpoint(ctx, volume)
	if err != nil {
		return err
	}
	if checkpoint == "" {
		checkpoint, err = s.bootstrapDefaultZygote(ctx, name, volume)
		if err != nil {
			return err
		}
	}
	return s.registerZygote(ctx, name, volume, checkpoint)
}

func (s *controlServer) hasRegisteredZygote(ctx context.Context, name string) (bool, error) {
	resp, err := s.sandboxAPI.do(ctx, http.MethodGet, "/v1/zygotes", nil, "")
	if err != nil {
		return false, err
	}
	defer util.SafeClose(resp.Body, "close zygote list response body")
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("list zygotes: %s", strings.TrimSpace(string(body)))
	}
	var records []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return false, err
	}
	for _, record := range records {
		if record.Name == name {
			return true, nil
		}
	}
	return false, nil
}

func (s *controlServer) latestCheckpoint(ctx context.Context, volume string) (string, error) {
	checkpoints, err := s.cli.listCheckpoints(ctx, volume)
	if err != nil {
		return "", err
	}
	if len(checkpoints) == 0 {
		return "", nil
	}
	return checkpoints[len(checkpoints)-1].ID, nil
}

func (s *controlServer) bootstrapDefaultZygote(ctx context.Context, name, volume string) (_ string, err error) {
	mountpoint := filepath.Join("/root/.loophole/bootstrap", name)
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", mountpoint, err)
	}

	// Spawn a per-volume owner: `loophole create` if new, `loophole mount` if exists.
	if err := s.spawnOwner(ctx, volume, mountpoint); err != nil {
		return "", err
	}
	ownerC := s.ownerClient(volume)
	mounted := true
	defer func() {
		if !mounted {
			return
		}
		if shutdownErr := ownerC.Shutdown(context.Background()); shutdownErr != nil {
			s.logger.Printf("default zygote cleanup failed volume=%s err=%v", volume, shutdownErr)
		}
	}()

	if err := prepareZygoteMount(mountpoint); err != nil {
		return "", err
	}
	if err := extractDefaultRootfs(ctx, mountpoint); err != nil {
		return "", err
	}

	cpID, err := ownerC.Checkpoint(ctx)
	if err != nil {
		return "", fmt.Errorf("checkpoint zygote: %w", err)
	}
	if err := ownerC.Shutdown(ctx); err != nil {
		s.logger.Printf("shutdown zygote owner: %v", err)
	}
	mounted = false
	return cpID, nil
}

// spawnOwner spawns a loophole owner process for the volume. It tries
// `loophole mount` first (volume exists) and falls back to `loophole create`
// (new volume). The process runs in the background with a VolumeSocket.
func (s *controlServer) spawnOwner(ctx context.Context, volume, mountpoint string) error {
	// Try mount first — succeeds if the volume already exists.
	args := s.cli.args("mount", volume, mountpoint)
	cmd := exec.CommandContext(ctx, s.cli.bin, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start owner for %s: %w", volume, err)
	}

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- cmd.Wait() }()

	// Wait for the owner socket to become ready.
	c := s.ownerClient(volume)
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return fmt.Errorf("owner for %s did not become ready", volume)
		case err := <-ownerDone:
			// Mount failed — volume probably doesn't exist. Try create.
			if err != nil {
				return s.spawnCreate(ctx, volume, mountpoint)
			}
			return fmt.Errorf("owner for %s exited unexpectedly", volume)
		default:
		}
		waitCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		if status, err := c.Status(waitCtx); err == nil && status.Mountpoint != "" {
			cancel()
			return nil
		}
		cancel()
		time.Sleep(200 * time.Millisecond)
	}
}

// spawnCreate spawns `loophole create <volume> -m <mountpoint>` which creates
// and mounts a new volume, staying as a foreground owner process.
func (s *controlServer) spawnCreate(ctx context.Context, volume, mountpoint string) error {
	args := s.cli.args("create", volume, "-m", mountpoint)
	cmd := exec.CommandContext(ctx, s.cli.bin, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("create owner for %s: %w", volume, err)
	}
	go func() { _ = cmd.Wait() }()

	c := s.ownerClient(volume)
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return fmt.Errorf("create owner for %s did not become ready", volume)
		default:
		}
		waitCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		if status, err := c.Status(waitCtx); err == nil && status.Mountpoint != "" {
			cancel()
			return nil
		}
		cancel()
		time.Sleep(200 * time.Millisecond)
	}
}

func (s *controlServer) registerZygote(ctx context.Context, name, volume, checkpoint string) error {
	body, _ := json.Marshal(map[string]string{
		"name":       name,
		"volume":     volume,
		"checkpoint": checkpoint,
	})
	resp, err := s.sandboxAPI.do(ctx, http.MethodPost, "/v1/zygotes", body, "application/json")
	if err != nil {
		return err
	}
	defer util.SafeClose(resp.Body, "close register zygote response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register zygote %q: %s", name, strings.TrimSpace(string(data)))
	}
	return nil
}

func prepareZygoteMount(mountpoint string) error {
	for _, dir := range []string{
		filepath.Join(mountpoint, "proc"),
		filepath.Join(mountpoint, "sys"),
		filepath.Join(mountpoint, "dev"),
		filepath.Join(mountpoint, "run"),
		filepath.Join(mountpoint, "tmp"),
		filepath.Join(mountpoint, "var", "tmp"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if err := os.Chmod(filepath.Join(mountpoint, "tmp"), 0o1777); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Join(mountpoint, "var", "tmp"), 0o1777); err != nil {
		return err
	}
	return nil
}

func extractDefaultRootfs(ctx context.Context, mountpoint string) error {
	archive := envOr("LOOPHOLE_DEFAULT_ZYGOTE_TAR", "/opt/loophole-rootfs/ubuntu-rootfs.tar")
	cmd := exec.CommandContext(ctx, "tar", "-xf", archive, "-C", mountpoint)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if out, err := cmd.Output(); err != nil {
		if len(out) > 0 {
			stderr.Write(out)
		}
		return fmt.Errorf("extract rootfs archive %s into %s: %w: %s", archive, mountpoint, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
