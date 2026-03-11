// Command import copies a raw disk image into a loophole volume.
//
// Usage:
//
//	go run ./cmd/import -config '{"local_dir":"/tmp/store","cache_dir":"/tmp/cache"}' -volume myvolume -image /path/to/rootfs.ext4
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/semistrict/loophole"
	"github.com/semistrict/loophole/internal/util"
	"github.com/semistrict/loophole/storage2"
)

type InitConfig struct {
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	LocalDir  string `json:"local_dir"`
	CacheDir  string `json:"cache_dir"`
}

func main() {
	configJSON := flag.String("config", "", "loophole config JSON")
	volumeName := flag.String("volume", "", "volume name to create/write")
	imagePath := flag.String("image", "", "path to raw disk image")
	flag.Parse()

	if *configJSON == "" || *volumeName == "" || *imagePath == "" {
		flag.Usage()
		os.Exit(1)
	}

	var cfg InitConfig
	if err := json.Unmarshal([]byte(*configJSON), &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}

	ctx := context.Background()

	var store loophole.ObjectStore
	var err error
	if cfg.LocalDir != "" {
		_ = os.MkdirAll(cfg.LocalDir, 0o755)
		store, err = loophole.NewFileStore(cfg.LocalDir)
	} else {
		inst := loophole.Instance{
			Bucket:    cfg.Bucket,
			Prefix:    cfg.Prefix,
			Endpoint:  cfg.Endpoint,
			Region:    cfg.Region,
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		}
		store, err = loophole.NewS3Store(ctx, inst)
	}
	if err != nil {
		log.Fatalf("create store: %v", err)
	}

	cacheDir := cfg.CacheDir
	if cacheDir == "" {
		cacheDir = "/tmp/loophole-cache"
	}
	_ = os.MkdirAll(cacheDir, 0o755)

	vm := storage2.NewVolumeManager(store, cacheDir, storage2.Config{}, nil, nil)

	// Open the disk image.
	f, err := os.Open(*imagePath)
	if err != nil {
		log.Fatalf("open image: %v", err)
	}
	defer util.SafeClose(f, "close image file")

	fi, err := f.Stat()
	if err != nil {
		log.Fatalf("stat image: %v", err)
	}
	imageSize := uint64(fi.Size())
	fmt.Printf("Image size: %d bytes (%.1f GiB)\n", imageSize, float64(imageSize)/(1<<30))

	// Create the volume (raw, no formatting).
	vol, err := vm.NewVolume(loophole.CreateParams{
		Volume:   *volumeName,
		Size:     imageSize,
		NoFormat: true,
	})
	if err != nil {
		log.Fatalf("create volume: %v", err)
	}

	// Copy data in 1 MiB chunks.
	const chunkSize = 1 << 20
	buf := make([]byte, chunkSize)
	var offset uint64
	var written uint64

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := vol.Write(buf[:n], offset); err != nil {
				log.Fatalf("write at offset %d: %v", offset, err)
			}
			offset += uint64(n)
			written += uint64(n)
			if written%(64<<20) == 0 {
				fmt.Printf("  wrote %d MiB / %d MiB\n", written>>20, imageSize>>20)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			log.Fatalf("read image: %v", readErr)
		}
	}

	fmt.Printf("Wrote %d bytes to volume %q\n", written, *volumeName)

	// Close flushes, waits for any in-flight compaction, and releases the lease.
	if err := vm.Close(ctx); err != nil {
		log.Fatalf("close: %v", err)
	}
	fmt.Println("Done.")
}
