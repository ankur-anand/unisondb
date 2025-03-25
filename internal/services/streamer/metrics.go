package streamer

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus Metrics.
var (
	metricsStreamSendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "unisondb",
		Subsystem: "streamer",
		Name:      "server_stream_send_duration_seconds",
		Help:      "Latency of streaming WAL records.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
	}, []string{"namespace", "method", "streamer"})

	metricsStreamSendTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "streamer",
		Name:      "server_record_send_total",
		Help:      "Total number of WAL record messages sent via streaming.",
	}, []string{"namespace", "method", "streamer"})

	metricsActiveStreamTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "unisondb",
		Subsystem: "streamer",
		Name:      "server_active_stream_count",
		Help:      "Total number of WAL record messages sent via streaming.",
	}, []string{"namespace", "method", "streamer"})

	metricsStreamSendErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "streamer",
		Name:      "server_send_errors_total",
		Help:      "Total number of errors during sending WAL records.",
	}, []string{"namespace", "method", "streamer"})

	clientWalRecvTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "unisondb",
			Subsystem: "streamer",
			Name:      "client_wal_entry_received_total",
			Help:      "Total number of WAL entry received from the gRPC stream.",
		},
		[]string{"namespace", "streamer"},
	)

	clientWalStreamErrTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "unisondb",
			Subsystem: "streamer",
			Name:      "client_stream_errors_total",
			Help:      "Total number of errors while streaming WAL at client side.",
		},
		[]string{"namespace", "streamer", "error"},
	)
)

// RegisterMetrics registers the Prometheus metrics.
func RegisterMetrics() {
	prometheus.MustRegister(
		metricsStreamSendLatency,
		metricsStreamSendTotal,
		metricsStreamSendErrors,
		metricsActiveStreamTotal,
		clientWalRecvTotal,
		clientWalStreamErrTotal)
}
