//go:build tinygo

package prometheus

import "github.com/prometheus/client_golang/prometheus/internal"

func NewGoCollector(opts ...func(o *internal.GoCollectorOptions)) Collector {
	return &noopCollector{}
}

type noopCollector struct{}

func (c *noopCollector) Describe(ch chan<- *Desc) {}
func (c *noopCollector) Collect(ch chan<- Metric) {}
