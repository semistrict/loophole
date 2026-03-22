package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/gorilla/websocket"
)

// TestWebSocketBidiNoCorruption verifies that WebSocket does NOT have the
// same corruption bug — full duplex works correctly.
func TestWebSocketBidiNoCorruption(t *testing.T) {
	const dataSize = 64 << 20 // 64MB
	original := make([]byte, dataSize)
	if _, err := rand.Read(original); err != nil {
		t.Fatal(err)
	}

	sock := fmt.Sprintf("/tmp/bidi-test-%d-ws.sock", os.Getpid())
	os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	defer os.Remove(sock)

	serverBuf := make([]byte, dataSize)
	serverDone := make(chan int, 1) // receives total bytes

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				serverDone <- -1
				return
			}
			defer conn.Close()

			total := 0
			msgs := 0
			for {
				mt, msg, err := conn.ReadMessage()
				if err != nil {
					break
				}
				if mt == websocket.CloseMessage {
					break
				}
				copy(serverBuf[total:], msg)
				total += len(msg)
				msgs++
				// Write back verbose output every ~1MB.
				if total%(1<<20) < len(msg) {
					conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("received %d bytes\n", total)))
				}
			}
			conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("done: %d\n", total)))
			serverDone <- total
		})
		http.Serve(ln, mux)
	}()

	// Client: dial websocket over Unix socket.
	dialer := websocket.Dialer{
		NetDialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", sock)
		},
	}
	conn, _, err := dialer.Dial("ws://test/upload", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Send data in 64KB chunks and read responses concurrently.
	go func() {
		for off := 0; off < dataSize; {
			end := off + 64*1024
			if end > dataSize {
				end = dataSize
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, original[off:end]); err != nil {
				return
			}
			off = end
		}
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}()

	// Read all responses (drain them).
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
	conn.Close()

	total := <-serverDone
	t.Logf("received %d / %d bytes", total, dataSize)

	if total != dataSize {
		t.Fatalf("size mismatch: got %d, want %d", total, dataSize)
	}
	if !bytes.Equal(original, serverBuf[:total]) {
		// Find divergence.
		for i := range total {
			if original[i] != serverBuf[i] {
				t.Fatalf("CORRUPTION at byte %d", i)
			}
		}
	}
	t.Log("PASS: WebSocket full-duplex — all bytes match")
}
