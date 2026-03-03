package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	CASAttempts = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cas",
		Name:      "attempts_total",
		Help:      "Total CAS (ModifyJSON) attempts including retries.",
	}))

	CASRetries = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cas",
		Name:      "retries_total",
		Help:      "CAS retries due to ETag conflict.",
	}))

	CASFailures = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "cas",
		Name:      "failures_total",
		Help:      "CAS operations that exhausted all retry attempts.",
	}))
)
