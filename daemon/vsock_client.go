//go:build linux

package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
)

type vsockExecClient struct {
	udsPath string
	port    uint32
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

type execRequest struct {
	Type string `json:"type"`
	Cmd  string `json:"cmd"`
}

func newVsockExecClient(udsPath string, port uint32) *vsockExecClient {
	return &vsockExecClient{udsPath: udsPath, port: port}
}

func (c *vsockExecClient) Exec(ctx context.Context, cmd string) (ExecResult, error) {
	conn, err := c.Dial(ctx)
	if err != nil {
		return ExecResult{}, err
	}
	defer func() { _ = conn.Close() }()

	if err := json.NewEncoder(conn).Encode(execRequest{Type: "exec", Cmd: cmd}); err != nil {
		return ExecResult{}, fmt.Errorf("encode exec request: %w", err)
	}

	var result ExecResult
	if err := json.NewDecoder(conn).Decode(&result); err != nil {
		return ExecResult{}, fmt.Errorf("decode exec response: %w", err)
	}
	return result, nil
}

func (c *vsockExecClient) Dial(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer
	rawConn, err := dialer.DialContext(ctx, "unix", c.udsPath)
	if err != nil {
		return nil, fmt.Errorf("dial vsock bridge %s: %w", c.udsPath, err)
	}

	reader := bufio.NewReader(rawConn)
	if _, err := fmt.Fprintf(rawConn, "CONNECT %d\n", c.port); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("write vsock connect request: %w", err)
	}

	ack, err := reader.ReadString('\n')
	if err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("read vsock connect ack: %w", err)
	}
	if !strings.HasPrefix(ack, "OK ") {
		_ = rawConn.Close()
		return nil, fmt.Errorf("unexpected vsock connect ack %q", strings.TrimSpace(ack))
	}

	return &bufferedConn{Conn: rawConn, reader: reader}, nil
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
