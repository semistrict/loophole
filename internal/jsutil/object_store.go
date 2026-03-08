//go:build js

package jsutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"syscall/js"

	"github.com/semistrict/loophole"
)

// JSObjectStore implements loophole.ObjectStore by calling JS functions.
// The JS side must set globalThis.__loophole_s3 to an object with methods:
//
//	get(key) → Promise<{body: Uint8Array, etag: string} | null>
//	getRange(key, offset, length) → Promise<{body: Uint8Array, etag: string} | null>
//	put(key, data: Uint8Array) → Promise<void>
//	putCAS(key, data: Uint8Array, etag: string) → Promise<string>  (returns new etag)
//	putIfNotExists(key, data: Uint8Array) → Promise<boolean>
//	del(key) → Promise<void>
//	list(prefix) → Promise<Array<{key: string, size: number}>>
type JSObjectStore struct {
	s3     js.Value
	prefix string
}

var _ loophole.ObjectStore = (*JSObjectStore)(nil)

// NewJSObjectStore creates an ObjectStore backed by JS.
// s3 must be the JS object implementing the methods above.
func NewJSObjectStore(s3 js.Value, prefix string) *JSObjectStore {
	return &JSObjectStore{s3: s3, prefix: prefix}
}

func (s *JSObjectStore) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + key
}

func (s *JSObjectStore) At(path string) loophole.ObjectStore {
	p := s.prefix + path
	if p != "" && p[len(p)-1] != '/' {
		p += "/"
	}
	return &JSObjectStore{s3: s.s3, prefix: p}
}

func (s *JSObjectStore) Get(_ context.Context, key string) (io.ReadCloser, string, error) {
	result, err := Await(s.s3.Call("get", s.fullKey(key)))
	if err != nil {
		return nil, "", err
	}
	if result.IsNull() || result.IsUndefined() {
		return nil, "", loophole.ErrNotFound
	}
	body := GoBytes(result.Get("body"))
	etag := result.Get("etag").String()
	return io.NopCloser(bytes.NewReader(body)), etag, nil
}

func (s *JSObjectStore) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	result, err := Await(s.s3.Call("getRange", s.fullKey(key), offset, length))
	if err != nil {
		return nil, "", err
	}
	if result.IsNull() || result.IsUndefined() {
		return nil, "", loophole.ErrNotFound
	}
	body := GoBytes(result.Get("body"))
	etag := result.Get("etag").String()
	return io.NopCloser(bytes.NewReader(body)), etag, nil
}

func (s *JSObjectStore) PutBytesCAS(_ context.Context, key string, data []byte, etag string) (string, error) {
	result, err := Await(s.s3.Call("putCAS", s.fullKey(key), JSBytes(data), etag))
	if err != nil {
		return "", err
	}
	return result.String(), nil
}

func (s *JSObjectStore) PutReader(_ context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	_, err = Await(s.s3.Call("put", s.fullKey(key), JSBytes(data)))
	return err
}

func (s *JSObjectStore) PutIfNotExists(_ context.Context, key string, data []byte) error {
	result, err := Await(s.s3.Call("putIfNotExists", s.fullKey(key), JSBytes(data)))
	if err != nil {
		return err
	}
	if !result.Bool() {
		return loophole.ErrExists
	}
	return nil
}

func (s *JSObjectStore) DeleteObject(_ context.Context, key string) error {
	_, err := Await(s.s3.Call("del", s.fullKey(key)))
	return err
}

func (s *JSObjectStore) ListKeys(_ context.Context, prefix string) ([]loophole.ObjectInfo, error) {
	fullPrefix := s.fullKey(prefix)
	result, err := Await(s.s3.Call("list", fullPrefix))
	if err != nil {
		return nil, err
	}
	length := result.Get("length").Int()
	infos := make([]loophole.ObjectInfo, length)
	for i := range length {
		item := result.Index(i)
		k := item.Get("key").String()
		// Strip the full prefix (store prefix + query prefix) to match
		// native S3Store behavior — callers get keys relative to the
		// prefix they asked for.
		if len(k) > len(fullPrefix) {
			k = k[len(fullPrefix):]
		}
		infos[i] = loophole.ObjectInfo{
			Key:  k,
			Size: int64(item.Get("size").Float()),
		}
	}
	return infos, nil
}

// MustGetS3 returns the globalThis.__loophole_s3 object or panics.
func MustGetS3() js.Value {
	v := js.Global().Get("__loophole_s3")
	if v.IsUndefined() || v.IsNull() {
		panic(fmt.Sprintf("globalThis.__loophole_s3 is not set — JS must provide the S3 adapter"))
	}
	return v
}
