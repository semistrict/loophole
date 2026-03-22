//go:build linux

package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

const (
	vsockHostCID     = 2
	vsockControlPort = 4041
)

// VsockClient talks to the loophole control port inside a Firecracker VM
// via AF_VSOCK. It speaks newline-delimited JSON.
type VsockClient struct{}

// NewVsock returns a vsock-backed client.
func NewVsock() *VsockClient { return &VsockClient{} }

type vsockRequest struct {
	Command string `json:"command"`
}

type vsockResponse struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
}

func (c *VsockClient) call(command string) (*vsockResponse, error) {
	conn, err := dialVsock(vsockHostCID, vsockControlPort)
	if err != nil {
		return nil, fmt.Errorf("vsock connect: %w", err)
	}
	defer conn.Close()

	req, _ := json.Marshal(vsockRequest{Command: command})
	req = append(req, '\n')
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("vsock write: %w", err)
	}

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("vsock read: %w", err)
		}
		return nil, fmt.Errorf("vsock: no response")
	}

	var resp vsockResponse
	if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("vsock: invalid response: %w", err)
	}

	if !resp.OK {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return &resp, nil
}

// Flush flushes the loophole volume via the vsock control port.
func (c *VsockClient) Flush() error {
	_, err := c.call("flush")
	return err
}

// Checkpoint creates a checkpoint via the vsock control port.
// Returns the checkpoint ID.
func (c *VsockClient) Checkpoint() (string, error) {
	resp, err := c.call("checkpoint")
	if err != nil {
		return "", err
	}
	var data struct {
		CheckpointID string `json:"checkpoint_id"`
	}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		return "", fmt.Errorf("parse checkpoint response: %w", err)
	}
	return data.CheckpointID, nil
}

// Stats returns volume debug info as raw JSON via the vsock control port.
func (c *VsockClient) Stats() (json.RawMessage, error) {
	resp, err := c.call("stats")
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func dialVsock(cid, port uint32) (net.Conn, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("socket: %w", err)
	}

	sa := &unix.SockaddrVM{CID: cid, Port: port}
	if err := unix.Connect(fd, sa); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("connect CID %d port %d: %w", cid, port, err)
	}

	f := os.NewFile(uintptr(fd), "vsock")
	conn, err := net.FileConn(f)
	f.Close() // FileConn dups the fd
	if err != nil {
		return nil, fmt.Errorf("fileconn: %w", err)
	}
	return conn, nil
}
