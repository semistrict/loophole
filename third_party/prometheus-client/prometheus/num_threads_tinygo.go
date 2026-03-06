//go:build tinygo

package prometheus

func getRuntimeNumThreads() float64 {
	return 1
}
