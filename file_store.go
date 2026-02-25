package loophole

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// FileStore implements ObjectStore backed by a local directory.
// Each key maps to a file under the root directory.
type FileStore struct {
	root   string
	prefix string
}

// NewFileStore creates a FileStore rooted at the given directory.
func NewFileStore(root string) (*FileStore, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileStore{root: root}, nil
}

func (f *FileStore) path(key string) string {
	full := key
	if f.prefix != "" {
		full = f.prefix + full
	}
	return filepath.Join(f.root, full)
}

func (f *FileStore) At(path string) ObjectStore {
	p := path + "/"
	if f.prefix != "" {
		p = f.prefix + p
	}
	return &FileStore{root: f.root, prefix: p}
}

func (f *FileStore) Get(_ context.Context, key string, offset int64) (io.ReadCloser, string, error) {
	p := f.path(key)
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, "", fmt.Errorf("get %s: %w", key, err)
	}
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	if offset > 0 {
		if int(offset) >= len(data) {
			data = nil
		} else {
			data = data[offset:]
		}
	}
	return io.NopCloser(bytes.NewReader(data)), etag, nil
}

func (f *FileStore) PutBytesCAS(_ context.Context, key string, data []byte, etag string) (string, error) {
	p := f.path(key)
	existing, err := os.ReadFile(p)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read for CAS %s: %w", key, err)
	}
	existingEtag := ""
	if err == nil {
		existingEtag = fmt.Sprintf(`"%x"`, sha256.Sum256(existing))
	}
	if existingEtag != etag {
		return "", fmt.Errorf("CAS conflict on %s: expected etag %s, got %s", key, etag, existingEtag)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return "", err
	}
	if err := os.WriteFile(p, data, 0644); err != nil {
		return "", fmt.Errorf("write %s: %w", key, err)
	}
	newEtag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	return newEtag, nil
}

func (f *FileStore) PutReader(_ context.Context, key string, r io.Reader) error {
	p := f.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0644)
}

func (f *FileStore) PutIfNotExists(_ context.Context, key string, data []byte) (bool, error) {
	p := f.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return false, err
	}
	// O_EXCL ensures atomic create-if-not-exists.
	file, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, err
	}
	_, err = file.Write(data)
	if cerr := file.Close(); cerr != nil {
		slog.Warn("close after write failed", "path", p, "error", cerr)
		if err == nil {
			err = cerr
		}
	}
	if err != nil {
		if rerr := os.Remove(p); rerr != nil {
			slog.Warn("remove failed", "path", p, "error", rerr)
		}
		return false, err
	}
	return true, nil
}

func (f *FileStore) DeleteObject(_ context.Context, key string) error {
	p := f.path(key)
	err := os.Remove(p)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (f *FileStore) ListKeys(_ context.Context, prefix string) ([]ObjectInfo, error) {
	fullPrefix := prefix
	if f.prefix != "" {
		fullPrefix = f.prefix + prefix
	}
	searchDir := filepath.Join(f.root, fullPrefix)

	var result []ObjectInfo
	err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(f.root, path)
		key := rel
		if f.prefix != "" {
			key = strings.TrimPrefix(rel, f.prefix)
		}
		if prefix != "" {
			key = strings.TrimPrefix(key, prefix)
		}
		result = append(result, ObjectInfo{Key: key, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
