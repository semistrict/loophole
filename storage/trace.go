package storage

import (
	"os"
	"strconv"
	"strings"
)

type traceConfig struct {
	layerPrefixes []string
	pageIndices   []PageIdx
}

var loopTrace traceConfig

func init() {
	loopTrace = parseTraceConfig(os.Getenv("LOOPHOLE_TRACE"))
}

func parseTraceConfig(spec string) traceConfig {
	var cfg traceConfig
	for _, token := range strings.Split(spec, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		kind, value, ok := strings.Cut(token, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		switch strings.TrimSpace(kind) {
		case "layer":
			cfg.layerPrefixes = append(cfg.layerPrefixes, value)
		case "page":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				continue
			}
			cfg.pageIndices = append(cfg.pageIndices, PageIdx(n))
		}
	}
	return cfg
}

func traceLayerEnabled(id string) bool {
	for _, prefix := range loopTrace.layerPrefixes {
		if strings.HasPrefix(id, prefix) {
			return true
		}
	}
	return false
}

func tracePageEnabled(pageIdx PageIdx) bool {
	if len(loopTrace.pageIndices) == 0 {
		return true
	}
	for _, p := range loopTrace.pageIndices {
		if p == pageIdx {
			return true
		}
	}
	return false
}
