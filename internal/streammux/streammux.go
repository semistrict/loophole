// Package streammux implements a simple binary multiplexing protocol for
// streaming stdout, stderr, and an exit code over a single byte stream.
//
// Frame format: [1 byte type][4 byte big-endian payload length][payload]
//
//   - Type 1 (Stdout): payload is written to stdout
//   - Type 2 (Stderr): payload is written to stderr
//   - Type 3 (Exit):   payload is a 4-byte big-endian int32 exit code
package streammux

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	TypeStdout byte = 1
	TypeStderr byte = 2
	TypeExit   byte = 3

	headerSize = 5 // 1 type + 4 length
	maxFrame   = 64 * 1024
)

// Writer multiplexes typed frames onto an underlying writer.
// It is safe for concurrent use.
type Writer struct {
	mu sync.Mutex
	w  io.Writer
}

// NewWriter creates a multiplexing writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// writeFrame writes a single frame. Caller must hold mu.
func (w *Writer) writeFrame(typ byte, data []byte) error {
	var hdr [headerSize]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(data)))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		_, err := w.w.Write(data)
		return err
	}
	return nil
}

// Stdout returns an io.Writer that sends stdout frames.
func (w *Writer) Stdout() io.Writer {
	return &streamWriter{mux: w, typ: TypeStdout}
}

// Stderr returns an io.Writer that sends stderr frames.
func (w *Writer) Stderr() io.Writer {
	return &streamWriter{mux: w, typ: TypeStderr}
}

// Exit sends an exit frame with the given code. This should be the last
// frame written.
func (w *Writer) Exit(code int32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(code))
	return w.writeFrame(TypeExit, buf[:])
}

type streamWriter struct {
	mux *Writer
	typ byte
}

func (sw *streamWriter) Write(p []byte) (int, error) {
	sw.mux.mu.Lock()
	defer sw.mux.mu.Unlock()

	written := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > maxFrame {
			chunk = chunk[:maxFrame]
		}
		if err := sw.mux.writeFrame(sw.typ, chunk); err != nil {
			return written, err
		}
		written += len(chunk)
		p = p[len(chunk):]
	}
	return written, nil
}

// Reader demultiplexes frames from an underlying reader.
type Reader struct {
	r io.Reader
}

// NewReader creates a demultiplexing reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// Frame is a single decoded frame.
type Frame struct {
	Type    byte
	Payload []byte
}

// ExitCode returns the exit code from an Exit frame.
func (f *Frame) ExitCode() int32 {
	if f.Type != TypeExit || len(f.Payload) < 4 {
		return -1
	}
	return int32(binary.BigEndian.Uint32(f.Payload))
}

// Next reads the next frame. Returns io.EOF when the stream ends.
func (r *Reader) Next() (*Frame, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r.r, hdr[:]); err != nil {
		return nil, err
	}
	typ := hdr[0]
	length := binary.BigEndian.Uint32(hdr[1:])
	if length > maxFrame+4 { // exit frame can be 4 bytes
		return nil, fmt.Errorf("streammux: frame too large: %d", length)
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r.r, payload); err != nil {
			return nil, fmt.Errorf("streammux: short payload: %w", err)
		}
	}
	return &Frame{Type: typ, Payload: payload}, nil
}

// Demux reads all frames, writing stdout/stderr to the provided writers,
// and returns the exit code from the Exit frame. If no Exit frame is
// received, returns -1.
func (r *Reader) Demux(stdout, stderr io.Writer) (int32, error) {
	for {
		frame, err := r.Next()
		if err == io.EOF {
			return -1, fmt.Errorf("streammux: unexpected EOF (no exit frame)")
		}
		if err != nil {
			return -1, err
		}
		switch frame.Type {
		case TypeStdout:
			if _, err := stdout.Write(frame.Payload); err != nil {
				return -1, err
			}
		case TypeStderr:
			if _, err := stderr.Write(frame.Payload); err != nil {
				return -1, err
			}
		case TypeExit:
			return frame.ExitCode(), nil
		default:
			return -1, fmt.Errorf("streammux: unknown frame type %d", frame.Type)
		}
	}
}
