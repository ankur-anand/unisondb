package replicator

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus Metrics.
var (
	metricsStreamSendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grpc_replicator_server_stream_send_observed_latency",
		Help:    "Latency of streaming WAL records.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	}, []string{"namespace", "method"})

	metricsStreamSendErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_replicator_server_send_errors_total",
		Help: "Total number of errors during sending WAL records.",
	}, []string{"namespace", "method"})

	metricsEOFTimeouts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "replicator_eof_timeouts_total",
		Help: "Total number of EOF timeouts during WAL record prefetching.",
	}, []string{"namespace"})

	clientWalRecvTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_replicator_client_wal_entry_received_total",
			Help: "Total number of WAL entry received from the gRPC stream.",
		},
		[]string{"namespace"},
	)

	clientWalStreamErrTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_replicator_client_stream_errors_total",
			Help: "Total number of errors while streaming WAL at client side.",
		},
		[]string{"namespace", "error"},
	)
)

// RegisterMetrics registers the Prometheus metrics.
func RegisterMetrics() {
	prometheus.MustRegister(metricsStreamSendLatency, metricsStreamSendErrors,
		metricsEOFTimeouts, clientWalRecvTotal,
		clientWalStreamErrTotal)
}
