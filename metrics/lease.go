package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	LeaseRenewals = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "lease",
		Name:      "renewals_total",
		Help:      "Lease renewal attempts.",
	}))

	LeaseErrors = reg(prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "loophole",
		Subsystem: "lease",
		Name:      "errors_total",
		Help:      "Failed lease renewals.",
	}))
)
