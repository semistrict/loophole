package blob

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

// FileDriver implements Driver backed by a local directory.
type FileDriver struct {
	root string
}

// NewFileDriver creates a FileDriver rooted at the given directory.
func NewFileDriver(root string) (*FileDriver, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve store dir: %w", err)
	}
	if err := os.MkdirAll(absRoot, 0755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileDriver{root: absRoot}, nil
}

func (d *FileDriver) path(key string) (string, error) {
	if filepath.IsAbs(key) {
		return "", fmt.Errorf("object key %q is absolute", key)
	}
	path := filepath.Join(d.root, key)
	rel, err := filepath.Rel(d.root, path)
	if err != nil {
		return "", fmt.Errorf("resolve object key %q: %w", key, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("object key %q escapes store root", key)
	}
	return path, nil
}

func (d *FileDriver) Get(_ context.Context, key string, opts GetOpts) (io.ReadCloser, int64, string, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, 0, "", err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, "", fmt.Errorf("get %s: %w", key, ErrNotFound)
		}
		return nil, 0, "", fmt.Errorf("get %s: %w", key, err)
	}
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	if opts.Length > 0 {
		if int(opts.Offset) >= len(data) {
			return io.NopCloser(bytes.NewReader(nil)), 0, etag, nil
		}
		end := opts.Offset + opts.Length
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		slice := data[opts.Offset:end]
		return io.NopCloser(bytes.NewReader(slice)), int64(len(slice)), etag, nil
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), etag, nil
}

func (d *FileDriver) Put(_ context.Context, key string, body io.Reader, opts PutOpts) (string, error) {
	p, err := d.path(key)
	if err != nil {
		return "", err
	}

	if opts.IfMatch != "" {
		// CAS: read existing, compare etag.
		existing, readErr := os.ReadFile(p)
		if readErr != nil && !os.IsNotExist(readErr) {
			return "", fmt.Errorf("read for CAS %s: %w", key, readErr)
		}
		existingEtag := ""
		if readErr == nil {
			existingEtag = fmt.Sprintf(`"%x"`, sha256.Sum256(existing))
		}
		if existingEtag != opts.IfMatch {
			return "", fmt.Errorf("CAS conflict on %s: expected etag %s, got %s", key, opts.IfMatch, existingEtag)
		}
	}

	if opts.IfNotExists {
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			return "", err
		}
		file, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			if os.IsExist(err) {
				return "", ErrExists
			}
			return "", err
		}
		data, readErr := io.ReadAll(body)
		if readErr != nil {
			util.SafeClose(file, "close new file")
			_ = os.Remove(p)
			return "", readErr
		}
		_, writeErr := file.Write(data)
		if cerr := file.Close(); cerr != nil {
			slog.Warn("close after write failed", "path", p, "error", cerr)
			if writeErr == nil {
				writeErr = cerr
			}
		}
		if writeErr != nil {
			_ = os.Remove(p)
			return "", writeErr
		}
		if opts.Metadata != nil {
			metaData, _ := json.Marshal(opts.Metadata)
			if werr := atomicWriteFile(p+".meta.json", metaData); werr != nil {
				slog.Warn("write metadata sidecar failed", "path", p+".meta.json", "error", werr)
			}
		}
		newEtag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
		return newEtag, nil
	}

	// Normal put (or CAS after etag check passed above).
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return "", err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	if err := atomicWriteFile(p, data); err != nil {
		return "", fmt.Errorf("write %s: %w", key, err)
	}
	newEtag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
	return newEtag, nil
}

func (d *FileDriver) Delete(_ context.Context, keys []string) error {
	for _, key := range keys {
		p, err := d.path(key)
		if err != nil {
			return err
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (d *FileDriver) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	searchDir, err := d.path(prefix)
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
		rel, _ := filepath.Rel(d.root, path)
		result = append(result, ObjectInfo{Key: rel, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (d *FileDriver) Head(_ context.Context, key string) (map[string]string, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, fmt.Errorf("head %s: %w", key, ErrNotFound)
	}
	metaPath := p + ".meta.json"
	data, err := os.ReadFile(metaPath)
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

func (d *FileDriver) SetMeta(_ context.Context, key string, meta map[string]string) error {
	p, err := d.path(key)
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
	return atomicWriteFile(p+".meta.json", data)
}

// atomicWriteFile writes data to path atomically by writing to a temp file
// in the same directory and then renaming.
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
