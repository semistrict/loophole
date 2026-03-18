package objstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/semistrict/loophole/internal/util"
)

// FileStore implements ObjectStore backed by a local directory.
// Each key maps to a file under the root directory.
type FileStore struct {
	root   string
	prefix string
}

// NewFileStore creates a FileStore rooted at the given directory.
func NewFileStore(root string) (*FileStore, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve store dir: %w", err)
	}
	if err := os.MkdirAll(absRoot, 0755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileStore{root: absRoot}, nil
}

func (f *FileStore) path(key string) (string, error) {
	full := key
	if f.prefix != "" {
		full = f.prefix + full
	}
	if filepath.IsAbs(full) {
		return "", fmt.Errorf("object key %q is absolute", key)
	}
	path := filepath.Join(f.root, full)
	rel, err := filepath.Rel(f.root, path)
	if err != nil {
		return "", fmt.Errorf("resolve object key %q: %w", key, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("object key %q escapes store root", key)
	}
	return path, nil
}

func (f *FileStore) At(path string) ObjectStore {
	p := path + "/"
	if f.prefix != "" {
		p = f.prefix + p
	}
	return &FileStore{root: f.root, prefix: p}
}

func (f *FileStore) Get(_ context.Context, key string) (io.ReadCloser, string, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
		}
		return nil, "", fmt.Errorf("get %s: %w", key, err)
	}
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	return io.NopCloser(bytes.NewReader(data)), etag, nil
}

func (f *FileStore) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, string, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
		}
		return nil, "", fmt.Errorf("get %s: %w", key, err)
	}
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	if int(offset) >= len(data) {
		return io.NopCloser(bytes.NewReader(nil)), etag, nil
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return io.NopCloser(bytes.NewReader(data[offset:end])), etag, nil
}

func (f *FileStore) PutBytesCAS(_ context.Context, key string, data []byte, etag string) (string, error) {
	p, err := f.path(key)
	if err != nil {
		return "", err
	}
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
	if err := atomicWriteFile(p, data); err != nil {
		return "", fmt.Errorf("write %s: %w", key, err)
	}
	newEtag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	return newEtag, nil
}

func (f *FileStore) PutReader(_ context.Context, key string, r io.Reader) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return atomicWriteFile(p, data)
}

func (f *FileStore) PutIfNotExists(_ context.Context, key string, data []byte, meta ...map[string]string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	// O_EXCL ensures atomic create-if-not-exists.
	file, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if os.IsExist(err) {
			return ErrExists
		}
		return err
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
		return err
	}
	if len(meta) > 0 && meta[0] != nil {
		metaData, _ := json.Marshal(meta[0])
		metaPath, pathErr := f.metaPath(key)
		if pathErr != nil {
			return pathErr
		}
		if werr := atomicWriteFile(metaPath, metaData); werr != nil {
			slog.Warn("write metadata sidecar failed", "path", metaPath, "error", werr)
		}
	}
	return nil
}

func (f *FileStore) DeleteObject(_ context.Context, key string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (f *FileStore) metaPath(key string) (string, error) {
	path, err := f.path(key)
	if err != nil {
		return "", err
	}
	return path + ".meta.json", nil
}

func (f *FileStore) HeadMeta(_ context.Context, key string) (map[string]string, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, fmt.Errorf("head %s: %w", key, ErrNotFound)
	}
	mp, err := f.metaPath(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(mp)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	var meta map[string]string
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (f *FileStore) SetMeta(_ context.Context, key string, meta map[string]string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return fmt.Errorf("set meta %s: %w", key, ErrNotFound)
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	mp, err := f.metaPath(key)
	if err != nil {
		return err
	}
	return atomicWriteFile(mp, data)
}

// atomicWriteFile writes data to path atomically by writing to a temp file
// in the same directory and then renaming. This prevents data loss if the
// process crashes mid-write (os.WriteFile truncates before writing, so a
// crash after truncation but before write completion leaves a corrupt file).
func atomicWriteFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		util.SafeClose(tmp, "close temp file after write failure")
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

func (f *FileStore) ListKeys(_ context.Context, prefix string) ([]ObjectInfo, error) {
	searchDir, err := f.path(prefix)
	if err != nil {
		return nil, err
	}

	var result []ObjectInfo
	err = filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() || strings.HasSuffix(path, ".meta.json") {
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
