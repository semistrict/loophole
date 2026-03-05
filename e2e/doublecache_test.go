//go:build linux

package e2e

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestE2E_DoubleCaching writes a unique random marker to a file on the ext4
// filesystem, fsyncs it, then checks how many copies of the marker exist in
// physical memory. More than one set of copies indicates that the same data
// is cached at multiple layers (e.g. the loophole process's mmap'd PageCache
// AND the kernel page cache for the FUSE/loop device).
//
// Approach: we scan /proc/self/pagemap to find physical frame numbers (PFNs)
// for all mapped virtual pages, then read /proc/self/mem to find which virtual
// pages contain the marker. Pages with the same PFN are the same physical page
// (shared mapping); pages with different PFNs are separate copies.
func TestE2E_DoubleCaching(t *testing.T) {
	skipKernelOnly(t)

	tfs, _ := mountVolume(t, "doublecache")

	// Generate a unique 128-byte random marker.
	marker := make([]byte, 128)
	_, err := rand.Read(marker)
	require.NoError(t, err)
	t.Logf("marker: %s", hex.EncodeToString(marker[:32])+"...")

	// Write a file containing the marker repeated to fill 1MB.
	const fileSize = 1024 * 1024
	data := make([]byte, fileSize)
	for off := 0; off+len(marker) <= len(data); off += len(marker) {
		copy(data[off:], marker)
	}
	tfs.WriteFile(t, "marker.bin", data)
	syncFS(t, tfs.mountpoint)

	// Read it back to populate read-path caches.
	readBack := tfs.ReadFile(t, "marker.bin")
	require.Equal(t, data, readBack)

	// Scan the process's virtual memory for the marker and collect the
	// physical frame numbers of pages that contain it.
	pfns := findMarkerPFNs(t, marker)

	uniquePFNs := make(map[uint64]int)
	for _, pfn := range pfns {
		uniquePFNs[pfn]++
	}

	t.Logf("virtual pages containing marker: %d", len(pfns))
	t.Logf("unique physical frames (PFNs):   %d", len(uniquePFNs))
	t.Logf("ratio (virt/phys): %.2f", float64(len(pfns))/float64(max(len(uniquePFNs), 1)))

	// If virt > phys, some virtual pages share the same physical frame
	// (e.g. shared mmap). If virt == phys, each copy is a distinct
	// physical page — indicating double-caching.
	//
	// Expected pages containing marker: fileSize/4096 = 256 pages per copy.
	// With DIRECT_IO:  ~256 pages (one copy in memLayer mmap)
	// Without DIRECT_IO + writeback: ~512+ pages (memLayer + kernel page cache)
	pagesPerCopy := fileSize / 4096
	copies := float64(len(pfns)) / float64(pagesPerCopy)
	t.Logf("estimated copies in memory: %.1f (%.0f pages / %d pages-per-copy)",
		copies, float64(len(pfns)), pagesPerCopy)
}

// findMarkerPFNs scans all readable mapped regions of the current process,
// finds pages containing the marker, and returns their physical frame numbers.
func findMarkerPFNs(t *testing.T, marker []byte) []uint64 {
	t.Helper()

	const pageSize = 4096

	maps, err := parseProcMaps()
	require.NoError(t, err)

	mem, err := os.Open("/proc/self/mem")
	require.NoError(t, err)
	defer mem.Close()

	pagemap, err := os.Open("/proc/self/pagemap")
	require.NoError(t, err)
	defer pagemap.Close()

	var pfns []uint64
	buf := make([]byte, pageSize)

	for _, m := range maps {
		if !m.readable {
			continue
		}

		for addr := m.start; addr < m.end; addr += pageSize {
			// Read the virtual page.
			n, err := mem.ReadAt(buf, int64(addr))
			if err != nil || n != pageSize {
				continue
			}

			// Check if this page contains the marker.
			if !bytes.Contains(buf, marker) {
				continue
			}

			// Look up the physical frame number via pagemap.
			pfn, present, err := readPagemapEntry(pagemap, addr, pageSize)
			if err != nil || !present {
				// Page not present or can't read pagemap — still count it
				// with a synthetic PFN based on virtual address.
				pfns = append(pfns, addr) // use vaddr as fallback
				continue
			}
			pfns = append(pfns, pfn)
		}
	}
	return pfns
}

// readPagemapEntry reads the pagemap entry for a virtual address and returns
// the physical frame number (PFN) and whether the page is present.
func readPagemapEntry(pagemap *os.File, vaddr, pageSize uint64) (pfn uint64, present bool, err error) {
	index := vaddr / pageSize
	var entry uint64
	err = binary.Read(
		io.NewSectionReader(pagemap, int64(index*8), 8),
		binary.LittleEndian,
		&entry,
	)
	if err != nil {
		return 0, false, err
	}

	present = entry&(1<<63) != 0
	if !present {
		return 0, false, nil
	}
	pfn = entry & ((1 << 55) - 1)
	return pfn, true, nil
}

type procMap struct {
	start, end uint64
	readable   bool
}

func parseProcMaps() ([]procMap, error) {
	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var maps []procMap
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		// Format: start-end perms offset dev inode pathname
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		addrs := strings.SplitN(parts[0], "-", 2)
		if len(addrs) != 2 {
			continue
		}
		start, err := strconv.ParseUint(addrs[0], 16, 64)
		if err != nil {
			continue
		}
		end, err := strconv.ParseUint(addrs[1], 16, 64)
		if err != nil {
			continue
		}
		perms := parts[1]
		maps = append(maps, procMap{
			start:    start,
			end:      end,
			readable: len(perms) > 0 && perms[0] == 'r',
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan /proc/self/maps: %w", err)
	}
	return maps, nil
}
