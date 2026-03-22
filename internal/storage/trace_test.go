package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTraceConfig(t *testing.T) {
	cfg := parseTraceConfig("layer:0000007f, nonsense, layer:abc123 , layer: , page:202, page:bogus")

	require.Equal(t, []string{"0000007f", "abc123"}, cfg.layerPrefixes)
	require.Equal(t, []PageIdx{202}, cfg.pageIndices)
}

func TestTraceLayerEnabled(t *testing.T) {
	prev := loopTrace
	t.Cleanup(func() { loopTrace = prev })

	loopTrace = parseTraceConfig("layer:0000007f,layer:deadbeef")

	require.True(t, traceLayerEnabled("0000007f-simtl-0000007f"))
	require.True(t, traceLayerEnabled("deadbeef-extra"))
	require.False(t, traceLayerEnabled("00000080-simtl-00000080"))
}

func TestTracePageEnabled(t *testing.T) {
	prev := loopTrace
	t.Cleanup(func() { loopTrace = prev })

	loopTrace = parseTraceConfig("page:202,page:17")

	require.True(t, tracePageEnabled(202))
	require.True(t, tracePageEnabled(17))
	require.False(t, tracePageEnabled(18))

	loopTrace = parseTraceConfig("layer:0000007f")
	require.True(t, tracePageEnabled(999))
}
