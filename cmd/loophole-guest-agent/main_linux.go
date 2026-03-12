//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"golang.org/x/sys/unix"
)

const guestAgentPort = 4040

type guestExecRequest struct {
	Type string `json:"type"`
	Cmd  string `json:"cmd"`
}

type guestExecResponse struct {
	ExitCode int    `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "guest agent: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	for _, mount := range []struct {
		target string
		source string
		fstype string
		data   string
	}{
		{target: "/proc", source: "proc", fstype: "proc"},
		{target: "/sys", source: "sysfs", fstype: "sysfs"},
		{target: "/dev", source: "devtmpfs", fstype: "devtmpfs", data: "mode=0755"},
	} {
		if err := os.MkdirAll(mount.target, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", mount.target, err)
		}
		if err := unix.Mount(mount.source, mount.target, mount.fstype, 0, mount.data); err != nil && err != syscall.EBUSY {
			return fmt.Errorf("mount %s: %w", mount.target, err)
		}
	}

	if err := setupNetworking(); err != nil {
		fmt.Fprintf(os.Stderr, "guest agent: networking setup failed: %v\n", err)
		// Continue without networking — exec still works over vsock.
	}

	ln, err := listenVsock(guestAgentPort)
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case sig := <-sigCh:
				return fmt.Errorf("stopping on signal %s", sig)
			default:
			}
			if isTemporaryAcceptErr(err) {
				continue
			}
			return fmt.Errorf("accept vsock connection: %w", err)
		}
		go handleConn(conn)
	}
}

func handleConn(conn ioReadWriteCloser) {
	defer func() { _ = conn.Close() }()

	var req guestExecRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		fmt.Fprintf(os.Stderr, "guest agent decode error: %v\n", err)
		return
	}
	if req.Type != "exec" {
		fmt.Fprintf(os.Stderr, "guest agent unsupported request: %s\n", req.Type)
		return
	}

	cmd := exec.Command("/bin/sh", "-lc", req.Cmd)
	cmd.Env = []string{
		"HOME=/root",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"TERM=xterm-256color",
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	exitCode := 0
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			stderr.WriteString(err.Error())
			stderr.WriteByte('\n')
			exitCode = 127
		}
	}

	if err := json.NewEncoder(conn).Encode(guestExecResponse{
		ExitCode: exitCode,
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
	}); err != nil {
		fmt.Fprintf(os.Stderr, "guest agent encode error: %v\n", err)
	}
}

type ioReadWriteCloser interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
}

type vsockListener struct {
	fd int
}

func listenVsock(port uint32) (*vsockListener, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("create vsock socket: %w", err)
	}

	sa := &unix.SockaddrVM{
		CID:  unix.VMADDR_CID_ANY,
		Port: port,
	}
	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("bind vsock socket: %w", err)
	}
	if err := unix.Listen(fd, 16); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("listen vsock socket: %w", err)
	}
	return &vsockListener{fd: fd}, nil
}

func (l *vsockListener) Accept() (ioReadWriteCloser, error) {
	fd, _, err := unix.Accept(l.fd)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "vsock-conn"), nil
}

func (l *vsockListener) Close() error {
	return unix.Close(l.fd)
}

func isTemporaryAcceptErr(err error) bool {
	return err == syscall.EINTR || err == syscall.EAGAIN || err == syscall.EWOULDBLOCK
}

// setupNetworking configures eth0 with a static IP and default route.
// This is needed because we run as init (no systemd/networkd/netplan).
func setupNetworking() error {
	ip := "/usr/sbin/ip"
	if _, err := os.Stat(ip); err != nil {
		ip = "/sbin/ip" // fallback for minimal rootfs
	}
	cmds := [][]string{
		{ip, "addr", "add", "172.16.0.2/24", "dev", "eth0"},
		{ip, "link", "set", "dev", "eth0", "up"},
		{ip, "route", "add", "default", "via", "172.16.0.1"},
	}
	for _, args := range cmds {
		cmd := exec.Command(args[0], args[1:]...)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("%v: %w (%s)", args, err, bytes.TrimSpace(out))
		}
	}
	fmt.Fprintf(os.Stderr, "guest agent: networking configured (172.16.0.2/24 via 172.16.0.1)\n")
	return nil
}
