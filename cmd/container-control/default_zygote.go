package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/semistrict/loophole/internal/util"
)

type checkpointListResponse struct {
	Checkpoints []struct {
		ID string `json:"id"`
	} `json:"checkpoints"`
}

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
	resp, err := s.daemonAPI.do(ctx, http.MethodGet, "/checkpoints?volume="+url.QueryEscape(volume), nil, "")
	if err != nil {
		return "", err
	}
	defer util.SafeClose(resp.Body, "close checkpoint list response body")
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("list checkpoints for %q: %s", volume, strings.TrimSpace(string(body)))
	}
	var payload checkpointListResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if len(payload.Checkpoints) == 0 {
		return "", nil
	}
	return payload.Checkpoints[len(payload.Checkpoints)-1].ID, nil
}

func (s *controlServer) bootstrapDefaultZygote(ctx context.Context, name, volume string) (_ string, err error) {
	if err := s.ensureVolumeExists(ctx, volume); err != nil {
		return "", err
	}

	mountpoint := filepath.Join("/root/.loophole/bootstrap", name)
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", mountpoint, err)
	}

	if err := s.mountVolume(ctx, volume, mountpoint); err != nil {
		return "", err
	}
	mounted := true
	defer func() {
		if !mounted {
			return
		}
		if unmountErr := s.unmountVolume(context.Background(), mountpoint); unmountErr != nil {
			s.logger.Printf("default zygote cleanup failed mountpoint=%s err=%v", mountpoint, unmountErr)
		}
	}()

	if err := prepareZygoteMount(mountpoint); err != nil {
		return "", err
	}
	if err := extractDefaultRootfs(ctx, mountpoint); err != nil {
		return "", err
	}

	checkpoint, err := s.checkpointMount(ctx, mountpoint)
	if err != nil {
		return "", err
	}
	if err := s.unmountVolume(ctx, mountpoint); err != nil {
		return "", err
	}
	mounted = false
	return checkpoint, nil
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

func (s *controlServer) mountVolume(ctx context.Context, volume, mountpoint string) error {
	body, _ := json.Marshal(map[string]string{"volume": volume, "mountpoint": mountpoint})
	resp, err := s.daemonAPI.do(ctx, http.MethodPost, "/mount", body, "application/json")
	if err != nil {
		return err
	}
	defer util.SafeClose(resp.Body, "close mount response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("mount %q at %q: %s", volume, mountpoint, strings.TrimSpace(string(data)))
	}
	return nil
}

func (s *controlServer) unmountVolume(ctx context.Context, mountpoint string) error {
	body, _ := json.Marshal(map[string]string{"mountpoint": mountpoint})
	resp, err := s.daemonAPI.do(ctx, http.MethodPost, "/unmount", body, "application/json")
	if err != nil {
		return err
	}
	defer util.SafeClose(resp.Body, "close unmount response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unmount %q: %s", mountpoint, strings.TrimSpace(string(data)))
	}
	return nil
}

func (s *controlServer) checkpointMount(ctx context.Context, mountpoint string) (string, error) {
	body, _ := json.Marshal(map[string]string{"mountpoint": mountpoint})
	resp, err := s.daemonAPI.do(ctx, http.MethodPost, "/checkpoint", body, "application/json")
	if err != nil {
		return "", err
	}
	defer util.SafeClose(resp.Body, "close checkpoint response body")
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("checkpoint %q: %s", mountpoint, strings.TrimSpace(string(data)))
	}
	var payload struct {
		Checkpoint string `json:"checkpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if payload.Checkpoint == "" {
		return "", fmt.Errorf("checkpoint %q: empty checkpoint id", mountpoint)
	}
	return payload.Checkpoint, nil
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
