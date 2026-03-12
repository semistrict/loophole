//go:build linux

package daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semistrict/loophole/internal/util"
)

type firecrackerVMManager struct {
	cfg firecrackerRuntimeConfig

	nextCID atomic.Uint32

	mu  sync.Mutex
	vms map[string]*managedFirecrackerVM
}

type managedFirecrackerVM struct {
	volume    string
	guestCID  uint32
	workDir   string
	apiSock   string
	vsockPath string
	netns     string // network namespace name (empty if none)

	cmd *exec.Cmd

	readyCh  chan struct{}
	readyErr error

	doneCh  chan struct{}
	exitErr error
}

type firecrackerConfigFile struct {
	BootSource        firecrackerBootSource         `json:"boot-source"`
	Drives            []firecrackerDrive            `json:"drives"`
	NetworkInterfaces []firecrackerNetworkInterface `json:"network-interfaces,omitempty"`
	Machine           firecrackerMachine            `json:"machine-config"`
	Vsock             firecrackerVsock              `json:"vsock"`
}

type firecrackerNetworkInterface struct {
	IfaceID     string `json:"iface_id"`
	HostDevName string `json:"host_dev_name"`
	GuestMAC    string `json:"guest_mac,omitempty"`
}

type firecrackerBootSource struct {
	KernelImagePath string `json:"kernel_image_path"`
	BootArgs        string `json:"boot_args"`
}

type firecrackerDrive struct {
	DriveID      string `json:"drive_id"`
	PathOnHost   string `json:"path_on_host"`
	IsRootDevice bool   `json:"is_root_device"`
	IsReadOnly   bool   `json:"is_read_only"`
	IOEngine     string `json:"io_engine"`
}

type firecrackerMachine struct {
	VCPUCount int `json:"vcpu_count"`
	MemMiB    int `json:"mem_size_mib"`
}

type firecrackerVsock struct {
	GuestCID uint32 `json:"guest_cid"`
	UDSPath  string `json:"uds_path"`
}

func newFirecrackerVMManager(cfg firecrackerRuntimeConfig) *firecrackerVMManager {
	mgr := &firecrackerVMManager{
		cfg: cfg,
		vms: make(map[string]*managedFirecrackerVM),
	}
	mgr.nextCID.Store(2)
	return mgr
}

func (m *firecrackerVMManager) HasRunning(volume string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	vm, ok := m.vms[volume]
	if !ok {
		return false
	}
	select {
	case <-vm.doneCh:
		delete(m.vms, volume)
		return false
	default:
		return true
	}
}

func (m *firecrackerVMManager) GetOrStart(ctx context.Context, volume string) (*managedFirecrackerVM, error) {
	m.mu.Lock()
	if vm, ok := m.vms[volume]; ok {
		m.mu.Unlock()
		return m.waitReady(ctx, vm)
	}

	vm := &managedFirecrackerVM{
		volume:   volume,
		guestCID: m.nextCID.Add(1),
		readyCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	m.vms[volume] = vm
	m.mu.Unlock()

	go m.startVM(vm)
	return m.waitReady(ctx, vm)
}

func (m *firecrackerVMManager) Close(ctx context.Context) error {
	m.mu.Lock()
	vms := make([]*managedFirecrackerVM, 0, len(m.vms))
	for _, vm := range m.vms {
		vms = append(vms, vm)
	}
	m.mu.Unlock()

	for _, vm := range vms {
		if err := m.stopVM(ctx, vm); err != nil {
			return err
		}
	}
	return nil
}

func (m *firecrackerVMManager) DebugInfo() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]map[string]any, 0, len(m.vms))
	for _, vm := range m.vms {
		state := "starting"
		select {
		case <-vm.doneCh:
			state = "stopped"
		default:
			select {
			case <-vm.readyCh:
				if vm.readyErr != nil {
					state = "error"
				} else {
					state = "ready"
				}
			default:
			}
		}
		out = append(out, map[string]any{
			"volume":     vm.volume,
			"guest_cid":  vm.guestCID,
			"state":      state,
			"work_dir":   vm.workDir,
			"api_sock":   vm.apiSock,
			"vsock_path": vm.vsockPath,
			"netns":      vm.netns,
			"log_path":   filepath.Join(vm.workDir, "firecracker.log"),
			"pid":        pidOf(vm.cmd),
			"ready_err":  errString(vm.readyErr),
			"exit_err":   errString(vm.exitErr),
		})
	}
	return out
}

func (m *firecrackerVMManager) DebugLog(volume string, lines int) (map[string]any, error) {
	logPath, err := m.logPathForVolume(volume)
	if err != nil {
		return nil, err
	}
	content, err := tailTextFile(logPath, lines)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"volume": volume,
		"path":   logPath,
		"log":    content,
	}, nil
}

func (m *firecrackerVMManager) waitReady(ctx context.Context, vm *managedFirecrackerVM) (*managedFirecrackerVM, error) {
	select {
	case <-vm.readyCh:
		if vm.readyErr != nil {
			return nil, vm.readyErr
		}
		return vm, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *firecrackerVMManager) startVM(vm *managedFirecrackerVM) {
	defer close(vm.readyCh)

	slog.Info("firecracker vm starting", "volume", vm.volume, "cid", vm.guestCID)
	workDir, err := m.prepareWorkDir(vm.volume)
	if err != nil {
		vm.readyErr = err
		return
	}

	vm.workDir = workDir
	vm.apiSock = filepath.Join(workDir, "firecracker.sock")
	vm.vsockPath = filepath.Join(workDir, "vsock.sock")
	configPath := filepath.Join(workDir, "config.json")
	logPath := filepath.Join(workDir, "firecracker.log")

	// Set up per-VM network namespace with tap device.
	nsName := fmt.Sprintf("fc-%d", vm.guestCID)
	tapDev := "tap0"
	if err := setupVMNetns(nsName, vm.guestCID); err != nil {
		vm.readyErr = fmt.Errorf("setup network namespace: %w", err)
		return
	}
	vm.netns = nsName

	cfg := firecrackerConfigFile{
		BootSource: firecrackerBootSource{
			KernelImagePath: m.cfg.kernelPath,
			BootArgs: strings.Join([]string{
				"console=ttyS0",
				"reboot=k",
				"panic=1",
				"pci=off",
				"root=/dev/vda",
				"rw",
				"init=/" + firecrackerGuestBinaryPath,
			}, " "),
		},
		Drives: []firecrackerDrive{{
			DriveID:      "rootfs",
			PathOnHost:   vm.volume,
			IsRootDevice: true,
			IsReadOnly:   false,
			IOEngine:     "Loophole",
		}},
		NetworkInterfaces: []firecrackerNetworkInterface{{
			IfaceID:     "eth0",
			HostDevName: tapDev,
			GuestMAC:    fmt.Sprintf("06:00:c0:a8:%02x:%02x", vm.guestCID>>8, vm.guestCID&0xff),
		}},
		Machine: firecrackerMachine{
			VCPUCount: m.cfg.vcpuCount,
			MemMiB:    m.cfg.memMiB,
		},
		Vsock: firecrackerVsock{
			GuestCID: vm.guestCID,
			UDSPath:  "vsock.sock", // relative so clones get their own socket via cmd.Dir
		},
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		vm.readyErr = fmt.Errorf("marshal firecracker config: %w", err)
		return
	}
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		vm.readyErr = fmt.Errorf("write firecracker config: %w", err)
		return
	}

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		vm.readyErr = fmt.Errorf("open firecracker log: %w", err)
		return
	}

	// Run firecracker inside its network namespace.
	cmd := exec.Command(
		"ip", "netns", "exec", nsName,
		m.cfg.firecrackerBin,
		"--api-sock", vm.apiSock,
		"--config-file", configPath,
		"--no-seccomp",
	)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(),
		"LOOPHOLE_INIT=1",
		"LOOPHOLE_CONFIG_DIR="+m.cfg.configDir,
		"LOOPHOLE_PROFILE="+m.cfg.profile,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		util.SafeClose(logFile, "close firecracker log file after start failure")
		vm.readyErr = fmt.Errorf("start firecracker: %w", err)
		return
	}
	vm.cmd = cmd
	slog.Info("firecracker vm process started", "volume", vm.volume, "pid", cmd.Process.Pid, "api_sock", vm.apiSock, "vsock", vm.vsockPath)

	go func() {
		vm.exitErr = cmd.Wait()
		util.SafeClose(logFile, "close firecracker log file")
		cleanupVMNetns(vm.netns)
		close(vm.doneCh)
		m.mu.Lock()
		delete(m.vms, vm.volume)
		m.mu.Unlock()
		if vm.exitErr != nil {
			slog.Warn("firecracker exited", "volume", vm.volume, "error", vm.exitErr)
		} else {
			slog.Info("firecracker exited", "volume", vm.volume)
		}
	}()

	bootCtx, cancel := context.WithTimeout(context.Background(), m.cfg.bootTimeout)
	defer cancel()
	slog.Info("firecracker vm waiting for guest", "volume", vm.volume, "timeout", m.cfg.bootTimeout.String())
	if err := waitForVsock(bootCtx, vm.vsockPath, m.cfg.vsockPort); err != nil {
		_ = m.stopVM(context.Background(), vm)
		vm.readyErr = fmt.Errorf("wait for firecracker guest on volume %q: %w", vm.volume, err)
		return
	}

	slog.Info("firecracker vm ready", "volume", vm.volume, "cid", vm.guestCID, "vsock", vm.vsockPath)
}

func (m *firecrackerVMManager) stopVM(ctx context.Context, vm *managedFirecrackerVM) error {
	if vm.cmd == nil || vm.cmd.Process == nil {
		cleanupVMNetns(vm.netns)
		return nil
	}

	_ = vm.cmd.Process.Signal(os.Interrupt)

	select {
	case <-vm.doneCh:
		cleanupVMNetns(vm.netns)
		return nil
	case <-ctx.Done():
		cleanupVMNetns(vm.netns)
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}

	if err := vm.cmd.Process.Kill(); err != nil && !strings.Contains(err.Error(), "process already finished") {
		cleanupVMNetns(vm.netns)
		return fmt.Errorf("kill firecracker vm %q: %w", vm.volume, err)
	}

	select {
	case <-vm.doneCh:
		cleanupVMNetns(vm.netns)
		return nil
	case <-ctx.Done():
		cleanupVMNetns(vm.netns)
		return ctx.Err()
	case <-time.After(5 * time.Second):
		cleanupVMNetns(vm.netns)
		return fmt.Errorf("timed out waiting for firecracker vm %q to exit", vm.volume)
	}
}

func waitForVsock(ctx context.Context, udsPath string, port uint32) error {
	client := newVsockExecClient(udsPath, port)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		conn, err := client.Dial(ctx)
		if err == nil {
			util.SafeClose(conn, "close firecracker vsock readiness probe")
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *firecrackerVMManager) logPathForVolume(volume string) (string, error) {
	m.mu.Lock()
	if vm, ok := m.vms[volume]; ok && vm.workDir != "" {
		m.mu.Unlock()
		return filepath.Join(vm.workDir, "firecracker.log"), nil
	}
	m.mu.Unlock()

	pattern := filepath.Join(m.cfg.workRoot, sanitizeVolumePrefix(volume)+"-*", "firecracker.log")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("glob firecracker logs for %q: %w", volume, err)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no firecracker log found for volume %q", volume)
	}
	slices.Sort(matches)
	return matches[len(matches)-1], nil
}

func tailTextFile(path string, lines int) (string, error) {
	if lines <= 0 {
		lines = 200
	}

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer util.SafeClose(f, "close firecracker log file")

	scanner := bufio.NewScanner(f)
	buf := make([]string, 0, lines)
	for scanner.Scan() {
		buf = append(buf, scanner.Text())
		if len(buf) > lines {
			buf = buf[1:]
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan %s: %w", path, err)
	}
	return strings.Join(buf, "\n"), nil
}

// prepareWorkDir creates a work directory for a Firecracker VM. If the
// computed path under workRoot would produce socket paths exceeding the
// Unix socket length limit (108 chars), a shorter temp directory is used.
func (m *firecrackerVMManager) prepareWorkDir(volume string) (string, error) {
	workDir := filepath.Join(m.cfg.workRoot, sanitizeVolumeName(volume))

	const maxSockName = 16 // len("firecracker.sock")
	const sunPathMax = 107 // SUN_LEN - 1 for null terminator
	if len(workDir)+1+maxSockName > sunPathMax {
		d, err := os.MkdirTemp("", "fc-vm-")
		if err != nil {
			return "", fmt.Errorf("create short firecracker workdir: %w", err)
		}
		slog.Info("firecracker vm: workdir too long for unix socket, using short path", "original", workDir, "short", d)
		workDir = d
	}

	if err := os.RemoveAll(workDir); err != nil {
		return "", fmt.Errorf("clear firecracker workdir: %w", err)
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", fmt.Errorf("create firecracker workdir: %w", err)
	}
	return workDir, nil
}

func sanitizeVolumePrefix(volume string) string {
	replacer := strings.NewReplacer("/", "_", string(os.PathSeparator), "_", ":", "_")
	return replacer.Replace(volume)
}

func sanitizeVolumeName(volume string) string {
	return sanitizeVolumePrefix(volume) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func pidOf(cmd *exec.Cmd) int {
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	return cmd.Process.Pid
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// fcAPI sends an HTTP request to a Firecracker API socket.
func fcAPI(sock, method, path string, body any) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal fcAPI body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, "http://localhost"+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create fcAPI request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sock)
			},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fcAPI %s %s: %w", method, path, err)
	}
	defer util.SafeClose(resp.Body, "close fcAPI response body")

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fcAPI read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("fcAPI %s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	return respBody, nil
}

// SnapshotVM pauses a running VM, creates a Firecracker snapshot, and resumes the original.
func (m *firecrackerVMManager) SnapshotVM(ctx context.Context, volume string) (*VMSnapshot, error) {
	m.mu.Lock()
	vm, ok := m.vms[volume]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no running VM for volume %q", volume)
	}

	// Wait for VM to be ready.
	select {
	case <-vm.readyCh:
		if vm.readyErr != nil {
			return nil, fmt.Errorf("VM for volume %q failed to start: %w", volume, vm.readyErr)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	slog.Info("snapshot: pausing VM", "volume", volume)
	if _, err := fcAPI(vm.apiSock, "PATCH", "/vm", map[string]string{"state": "Paused"}); err != nil {
		return nil, fmt.Errorf("pause VM: %w", err)
	}

	snapPath := filepath.Join(vm.workDir, "snap")
	memPath := filepath.Join(vm.workDir, "mem")
	memVolName := fmt.Sprintf("%s-mem-%d", volume, time.Now().Unix())

	slog.Info("snapshot: creating snapshot", "volume", volume, "snap_path", snapPath, "mem_volume", memVolName)
	if _, err := fcAPI(vm.apiSock, "PUT", "/snapshot/create", map[string]any{
		"snapshot_type":   "Full",
		"snapshot_path":   snapPath,
		"mem_file_path":   memPath,
		"mem_volume_name": memVolName,
	}); err != nil {
		// Try to resume even if snapshot fails.
		_, _ = fcAPI(vm.apiSock, "PATCH", "/vm", map[string]string{"state": "Resumed"})
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	slog.Info("snapshot: resuming VM", "volume", volume)
	if _, err := fcAPI(vm.apiSock, "PATCH", "/vm", map[string]string{"state": "Resumed"}); err != nil {
		return nil, fmt.Errorf("resume VM: %w", err)
	}

	// Read the sidecar file that FC writes with the cloned memory volume name.
	sidecarPath := snapPath + ".mem-clone"
	sidecarData, err := os.ReadFile(sidecarPath)
	if err != nil {
		return nil, fmt.Errorf("read memory clone sidecar %s: %w", sidecarPath, err)
	}
	memCloneVolume := strings.TrimSpace(string(sidecarData))

	slog.Info("snapshot: complete", "volume", volume, "mem_clone_volume", memCloneVolume)
	return &VMSnapshot{
		SnapshotPath:   snapPath,
		MemCloneVolume: memCloneVolume,
		SourceVolume:   volume,
	}, nil
}

// RestoreVM restores a snapshot into a new Firecracker VM in its own netns.
func (m *firecrackerVMManager) RestoreVM(ctx context.Context, snap *VMSnapshot, cloneName string) (*VMCloneResult, error) {
	m.mu.Lock()
	if _, exists := m.vms[cloneName]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("VM already exists for volume %q", cloneName)
	}

	vm := &managedFirecrackerVM{
		volume:   cloneName,
		guestCID: m.nextCID.Add(1),
		readyCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	m.vms[cloneName] = vm
	m.mu.Unlock()

	go m.restoreVM(vm, snap)

	select {
	case <-vm.readyCh:
		if vm.readyErr != nil {
			return nil, vm.readyErr
		}
		return &VMCloneResult{
			Volume:   cloneName,
			GuestCID: vm.guestCID,
			Netns:    vm.netns,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *firecrackerVMManager) restoreVM(vm *managedFirecrackerVM, snap *VMSnapshot) {
	defer close(vm.readyCh)

	slog.Info("restore: starting clone VM", "clone", vm.volume, "source", snap.SourceVolume, "cid", vm.guestCID)

	workDir, err := m.prepareWorkDir(vm.volume)
	if err != nil {
		vm.readyErr = err
		return
	}

	vm.workDir = workDir
	vm.apiSock = filepath.Join(workDir, "firecracker.sock")
	vm.vsockPath = filepath.Join(workDir, "vsock.sock")
	logPath := filepath.Join(workDir, "firecracker.log")

	// Set up per-VM network namespace.
	nsName := fmt.Sprintf("fc-%d", vm.guestCID)
	if err := setupVMNetns(nsName, vm.guestCID); err != nil {
		vm.readyErr = fmt.Errorf("setup clone network namespace: %w", err)
		return
	}
	vm.netns = nsName

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		vm.readyErr = fmt.Errorf("open clone log file: %w", err)
		return
	}

	// Start Firecracker in restore mode (no --config-file).
	cmd := exec.Command(
		"ip", "netns", "exec", nsName,
		m.cfg.firecrackerBin,
		"--api-sock", vm.apiSock,
		"--no-seccomp",
	)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(),
		"LOOPHOLE_INIT=1",
		"LOOPHOLE_CONFIG_DIR="+m.cfg.configDir,
		"LOOPHOLE_PROFILE="+m.cfg.profile,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		util.SafeClose(logFile, "close clone log file after start failure")
		vm.readyErr = fmt.Errorf("start clone firecracker: %w", err)
		return
	}
	vm.cmd = cmd
	slog.Info("restore: clone process started", "clone", vm.volume, "pid", cmd.Process.Pid)

	go func() {
		vm.exitErr = cmd.Wait()
		util.SafeClose(logFile, "close clone firecracker log file")
		cleanupVMNetns(vm.netns)
		close(vm.doneCh)
		m.mu.Lock()
		delete(m.vms, vm.volume)
		m.mu.Unlock()
		if vm.exitErr != nil {
			slog.Warn("clone firecracker exited", "clone", vm.volume, "error", vm.exitErr)
		} else {
			slog.Info("clone firecracker exited", "clone", vm.volume)
		}
	}()

	// Wait for the API socket to appear.
	slog.Info("restore: waiting for API socket", "clone", vm.volume)
	sockCtx, sockCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer sockCancel()
	if err := waitForSocket(sockCtx, vm.apiSock); err != nil {
		_ = m.stopVM(context.Background(), vm)
		vm.readyErr = fmt.Errorf("wait for clone API socket: %w", err)
		return
	}

	// Load snapshot into the clone.
	slog.Info("restore: loading snapshot", "clone", vm.volume, "mem_clone_volume", snap.MemCloneVolume)
	if _, err := fcAPI(vm.apiSock, "PUT", "/snapshot/load", map[string]any{
		"snapshot_path": snap.SnapshotPath,
		"mem_backend": map[string]string{
			"backend_type": "Loophole",
			"backend_path": snap.MemCloneVolume,
		},
		"enable_diff_snapshots": true,
		"resume_vm":             true,
	}); err != nil {
		_ = m.stopVM(context.Background(), vm)
		vm.readyErr = fmt.Errorf("load snapshot into clone: %w", err)
		return
	}

	// Wait for guest agent on vsock.
	slog.Info("restore: waiting for guest agent", "clone", vm.volume)
	bootCtx, bootCancel := context.WithTimeout(context.Background(), m.cfg.bootTimeout)
	defer bootCancel()
	if err := waitForVsock(bootCtx, vm.vsockPath, m.cfg.vsockPort); err != nil {
		_ = m.stopVM(context.Background(), vm)
		vm.readyErr = fmt.Errorf("wait for clone guest agent on volume %q: %w", vm.volume, err)
		return
	}

	slog.Info("restore: clone VM ready", "clone", vm.volume, "cid", vm.guestCID)
}

// waitForSocket polls until a Unix socket file appears.
func waitForSocket(ctx context.Context, path string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if fi, err := os.Stat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("socket %s did not appear: %w", path, ctx.Err())
		case <-ticker.C:
		}
	}
}

// setupVMNetns creates a network namespace for a Firecracker VM with:
//   - A veth pair connecting the namespace to the host
//   - A tap device inside the namespace for Firecracker
//   - NAT and forwarding so the guest can reach the internet
//
// Network layout per VM (CID used for unique /24 subnet):
//
//	host:   veth-fc-<cid>  10.0.<cid>.1/24
//	netns:  veth-ns        10.0.<cid>.2/24   (+ tap0 172.16.0.1/24 for FC guest)
//	guest:  eth0           172.16.0.2/24     (configured by guest agent)
//
// Traffic: guest → tap0 → netns routing → veth → host → NAT → internet
func setupVMNetns(nsName string, guestCID uint32) error {
	// Use low byte of CID for subnet. CID starts at 3 (2 is reserved).
	subnet := guestCID & 0xff
	hostVeth := fmt.Sprintf("veth-fc-%d", subnet)
	nsVeth := "veth-ns"
	hostIP := fmt.Sprintf("10.0.%d.1/24", subnet)
	nsIP := fmt.Sprintf("10.0.%d.2/24", subnet)
	hostGW := fmt.Sprintf("10.0.%d.1", subnet)

	// Clean up any leftover namespace with the same name.
	cleanupVMNetns(nsName)

	cmds := [][]string{
		// Create namespace.
		{"ip", "netns", "add", nsName},
		// Create veth pair.
		{"ip", "link", "add", hostVeth, "type", "veth", "peer", "name", nsVeth},
		// Move namespace end into the netns.
		{"ip", "link", "set", nsVeth, "netns", nsName},
		// Configure host end.
		{"ip", "addr", "add", hostIP, "dev", hostVeth},
		{"ip", "link", "set", hostVeth, "up"},
		// Configure namespace end.
		{"ip", "netns", "exec", nsName, "ip", "addr", "add", nsIP, "dev", nsVeth},
		{"ip", "netns", "exec", nsName, "ip", "link", "set", nsVeth, "up"},
		{"ip", "netns", "exec", nsName, "ip", "link", "set", "lo", "up"},
		{"ip", "netns", "exec", nsName, "ip", "route", "add", "default", "via", hostGW},
		// Create tap device inside namespace for Firecracker.
		{"ip", "netns", "exec", nsName, "ip", "tuntap", "add", "dev", "tap0", "mode", "tap"},
		{"ip", "netns", "exec", nsName, "ip", "addr", "add", "172.16.0.1/24", "dev", "tap0"},
		{"ip", "netns", "exec", nsName, "ip", "link", "set", "tap0", "up"},
		// Enable forwarding and NAT inside the namespace (tap0 → veth-ns).
		{"ip", "netns", "exec", nsName, "sysctl", "-w", "net.ipv4.ip_forward=1"},
		{"ip", "netns", "exec", nsName, "iptables", "-t", "nat", "-A", "POSTROUTING", "-o", nsVeth, "-j", "MASQUERADE"},
		{"ip", "netns", "exec", nsName, "iptables", "-A", "FORWARD", "-i", "tap0", "-o", nsVeth, "-j", "ACCEPT"},
		{"ip", "netns", "exec", nsName, "iptables", "-A", "FORWARD", "-i", nsVeth, "-o", "tap0", "-j", "ACCEPT"},
	}

	for _, args := range cmds {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err != nil {
			// Best-effort cleanup on failure.
			cleanupVMNetns(nsName)
			return fmt.Errorf("%v: %w (%s)", args, err, strings.TrimSpace(string(out)))
		}
	}

	// Enable NAT on the host for this subnet.
	hostNATCmds := [][]string{
		{"sysctl", "-w", "net.ipv4.ip_forward=1"},
		{"iptables", "-t", "nat", "-A", "POSTROUTING", "-s", fmt.Sprintf("10.0.%d.0/24", subnet), "!", "-o", hostVeth, "-j", "MASQUERADE"},
	}
	for _, args := range hostNATCmds {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err != nil {
			cleanupVMNetns(nsName)
			return fmt.Errorf("host NAT %v: %w (%s)", args, err, strings.TrimSpace(string(out)))
		}
	}

	slog.Info("firecracker network namespace ready", "netns", nsName, "host_veth", hostVeth, "subnet", fmt.Sprintf("10.0.%d.0/24", subnet))
	return nil
}

func cleanupVMNetns(nsName string) {
	if nsName == "" {
		return
	}
	// Deleting the netns removes the veth pair and all iptables rules inside it.
	out, err := exec.Command("ip", "netns", "delete", nsName).CombinedOutput()
	if err != nil {
		// Not an error if it doesn't exist.
		if !strings.Contains(string(out), "No such file") {
			slog.Warn("failed to delete network namespace", "netns", nsName, "error", err, "output", strings.TrimSpace(string(out)))
		}
	} else {
		slog.Info("deleted network namespace", "netns", nsName)
	}

	// Clean up host-side NAT rule (iptables rules are not removed when netns is deleted).
	subnet := 0
	if _, err := fmt.Sscanf(nsName, "fc-%d", &subnet); err == nil {
		_ = exec.Command("iptables", "-t", "nat", "-D", "POSTROUTING", "-s", fmt.Sprintf("10.0.%d.0/24", subnet), "!", "-o", fmt.Sprintf("veth-fc-%d", subnet), "-j", "MASQUERADE").Run()
	}
}
