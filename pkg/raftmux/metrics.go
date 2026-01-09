package raftmux

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricsActiveSessionsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "active_sessions_total",
		Help:      "Current number of active yamux sessions.",
	})

	metricsActiveNamespacesTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "active_namespaces_total",
		Help:      "Current number of registered namespaces.",
	})

	metricsDialTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "dial_total",
		Help:      "Total number of dial attempts.",
	}, []string{"namespace", "status"})

	metricsAcceptTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "accept_total",
		Help:      "Total number of accepted connections.",
	}, []string{"namespace", "status"})

	metricsStreamRoutingLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "stream_routing_duration_seconds",
		Help:      "Time to route an incoming stream to its namespace.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 10),
	})

	metricsHeaderReadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "header_read_errors_total",
		Help:      "Total number of errors reading namespace headers.",
	})

	metricsUnknownNamespaceTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "unknown_namespace_total",
		Help:      "Total number of streams with unknown namespace headers.",
	})

	metricsAcceptBacklogFull = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "raftmux",
		Name:      "accept_backlog_full_total",
		Help:      "Total number of times accept backlog was full.",
	}, []string{"namespace"})
)

func RegisterMetrics() {
	prometheus.MustRegister(
		metricsActiveSessionsTotal,
		metricsActiveNamespacesTotal,
		metricsDialTotal,
		metricsAcceptTotal,
		metricsStreamRoutingLatency,
		metricsHeaderReadErrors,
		metricsUnknownNamespaceTotal,
		metricsAcceptBacklogFull,
	)
}
