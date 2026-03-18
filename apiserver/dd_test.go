package apiserver

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/semistrict/loophole/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowReader wraps a reader but returns at most maxPerRead bytes per Read call,
// simulating chunked HTTP transport that delivers data in small pieces.
type slowReader struct {
	r          io.Reader
	maxPerRead int
}

func (s *slowReader) Read(p []byte) (int, error) {
	if len(p) > s.maxPerRead {
		p = p[:s.maxPerRead]
	}
	return s.r.Read(p)
}

func TestDDWrite_ReadFull_ViaHTTP(t *testing.T) {
	data := make([]byte, storage.BlockSize)
	for i := range data {
		data[i] = byte(i % 199)
	}

	var gotData []byte
	var gotErr error

	// Simulate the server-side ReadFull behavior from handleDeviceDDWrite.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, storage.BlockSize)
		n, err := io.ReadFull(r.Body, buf)
		gotErr = err
		gotData = make([]byte, n)
		copy(gotData, buf[:n])
		w.WriteHeader(204)
	})

	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Wrap in a slowReader to simulate chunked delivery (1KB at a time).
	sr := &slowReader{r: bytes.NewReader(data), maxPerRead: 1024}
	resp, err := http.Post(srv.URL, "application/octet-stream", sr)
	require.NoError(t, err)
	resp.Body.Close()

	require.NoError(t, gotErr)
	assert.Equal(t, storage.BlockSize, len(gotData))
	assert.Equal(t, data, gotData)
}

func TestDDWrite_PartialBlock_ViaHTTP(t *testing.T) {
	// Last block of an image may be smaller than BlockSize.
	data := make([]byte, 1234)
	for i := range data {
		data[i] = byte(i % 101)
	}

	var gotData []byte
	var gotErr error

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, storage.BlockSize)
		n, err := io.ReadFull(r.Body, buf)
		gotErr = err
		gotData = make([]byte, n)
		copy(gotData, buf[:n])
		w.WriteHeader(204)
	})

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/octet-stream", bytes.NewReader(data))
	require.NoError(t, err)
	resp.Body.Close()

	// ReadFull returns ErrUnexpectedEOF for partial reads — that's expected
	// for the last block.
	assert.ErrorIs(t, gotErr, io.ErrUnexpectedEOF)
	assert.Equal(t, len(data), len(gotData))
	assert.Equal(t, data, gotData)
}
