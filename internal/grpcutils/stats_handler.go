package grpcutils

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/helpers/templates"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type contextKey string

const (
	promNamespace                  = "unisondb"
	promSubSystem                  = "grpc"
	strUnknown                     = "unknown"
	ctxKeyRemoteAddr    contextKey = "remote_addr"
	ctxKeyConnStartTime contextKey = "conn_start_time"
	ctxKeyRPCService    contextKey = "rpc_service"
	ctxKeyRPCMethod     contextKey = "rpc_method"
	ctxKeyRPCID         contextKey = "rpc_id"
)

// GRPCStatsHandler Implements grpc stats handler for monitoring purposes.
type GRPCStatsHandler struct {
	// track all the active streams
	activeStreamsMap  sync.Map
	activeStreamCount *prometheus.GaugeVec
	streamAgeBucket   *prometheus.GaugeVec

	methodInfo map[string]string
	// activeConn is grpc(tcp level) connection.
	activeConn     *prometheus.GaugeVec
	connTotalCount *prometheus.CounterVec

	rpcHandled      *prometheus.CounterVec
	rpcDuration     *prometheus.HistogramVec
	rpcInFlight     *prometheus.GaugeVec
	rpcErrorCounter *prometheus.CounterVec

	messageSize *prometheus.HistogramVec
	sentMessage *prometheus.CounterVec
	recvMessage *prometheus.CounterVec

	// these need to have a large vector or summary ?
	connDuration *prometheus.HistogramVec
}

type streamInfo struct {
	service    string
	method     string
	start      time.Time
	streamType string
}

func NewGRPCStatsHandler(methodInfo map[string]string) *GRPCStatsHandler {
	return &GRPCStatsHandler{
		methodInfo:       methodInfo,
		activeStreamsMap: sync.Map{},

		activeConn: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "active_connections",
			Help:      "Number of active grpc connections",
		}, nil),

		connTotalCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "connections_total",
			Help:      "Total number of grpc connections established",
		}, nil),

		connDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "connection_duration_seconds",
			Help:      "Duration of grpc connections in seconds",
			Buckets: []float64{
				1, 5, 10, 30, 60, // short duration connections.
				300, 600, 900, 1800, // minutes long connections.
				3600, 7200, 14400, 28800, 43200, 86400}, // hour long connections.
		}, nil),

		rpcHandled: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "rpc_handled_total",
			Help:      "Total number of rpc Started/Handled",
		}, []string{"grpc_service", "grpc_method", "grpc_type"}),

		rpcDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "rpc_duration_seconds",
			Help:      "Duration of completed rpc in seconds",
		}, []string{"grpc_service", "grpc_method", "grpc_type", "status_code"}),

		rpcInFlight: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "rpc_inflight_total",
			Help:      "Total number of rpc In Flight",
		}, []string{"grpc_service", "grpc_method", "grpc_type"}),

		rpcErrorCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "rpc_error_total",
			Help:      "Total number of RPC Errors",
		}, []string{"grpc_service", "grpc_method", "grpc_type", "status_code"}),

		recvMessage: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "recv_message_total",
			Help:      "Total number of received message",
		}, []string{"grpc_service", "grpc_method", "grpc_type"}),

		sentMessage: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "sent_message_total",
			Help:      "Total number of sent message",
		}, []string{"grpc_service", "grpc_method", "grpc_type"}),

		messageSize: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "rpc_message_size_bytes",
			Help:      "Size of gRPC message in bytes (direction)",
			// 1KB to 2MB
			Buckets: prometheus.ExponentialBuckets(1024, 2, 12),
		}, []string{"grpc_service", "grpc_method", "grpc_type", "direction"}),

		activeStreamCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "active_streams",
			Help:      "Number of currently active grpc streams",
		}, []string{"grpc_service", "grpc_method", "stream_type"}),

		streamAgeBucket: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: promNamespace,
			Subsystem: promSubSystem,
			Name:      "streams_by_age_bucket",
			Help:      "Number of currently active grpc streams by age bucket",
		}, []string{"grpc_service", "grpc_method", "stream_type", "age_bucket"}),
	}
}

func (h *GRPCStatsHandler) TagRPC(ctx context.Context, st *stats.RPCTagInfo) context.Context {
	rpcID := uuid.New().String()

	service, method := parseFullMethodName(st.FullMethodName)
	ctx = context.WithValue(ctx, ctxKeyRPCMethod, method)
	ctx = context.WithValue(ctx, ctxKeyRPCService, service)
	ctx = context.WithValue(ctx, ctxKeyRPCID, rpcID)
	return ctx
}

func (h *GRPCStatsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	service, _ := ctx.Value(ctxKeyRPCService).(string)
	method, _ := ctx.Value(ctxKeyRPCMethod).(string)
	rpcID, _ := ctx.Value(ctxKeyRPCID).(string)

	methodType := "unary"
	fullMethod := fmt.Sprintf("/%s/%s", service, method)
	if mType, ok := h.methodInfo[fullMethod]; ok {
		methodType = mType
	}

	switch rpcStats := rs.(type) {
	case *stats.Begin:
		h.rpcHandled.WithLabelValues(service, method, methodType).Inc()
		h.rpcInFlight.WithLabelValues(service, method, methodType).Inc()

		if methodType != unary {
			h.activeStreamsMap.Store(rpcID, streamInfo{
				service:    service,
				method:     method,
				start:      rpcStats.BeginTime,
				streamType: methodType,
			})
			h.activeStreamCount.WithLabelValues(service, method, methodType).Inc()
		}

	case *stats.End:
		statusCode := getStatusCode(rpcStats.Error)
		if rpcStats.Error != nil {
			h.rpcErrorCounter.WithLabelValues(service, method, methodType, statusCode).Inc()
		}
		h.rpcInFlight.WithLabelValues(service, method, methodType).Dec()
		h.rpcDuration.WithLabelValues(service, method, methodType, statusCode).Observe(rpcStats.EndTime.Sub(rpcStats.BeginTime).Seconds())
		if methodType != unary {
			h.activeStreamsMap.Delete(rpcID)
			h.activeStreamCount.WithLabelValues(service, method, methodType).Dec()
		}

	case *stats.InPayload:
		h.recvMessage.WithLabelValues(service, method, methodType).Inc()
		h.messageSize.WithLabelValues(service, method, methodType, "received").Observe(float64(rpcStats.WireLength))
	case *stats.OutPayload:
		h.sentMessage.WithLabelValues(service, method, methodType).Inc()
		h.messageSize.WithLabelValues(service, method, methodType, "sent").Observe(float64(rpcStats.WireLength))
	}
}

func getStatusCode(err error) string {
	if err == nil {
		return codes.OK.String()
	}

	st, ok := status.FromError(err)
	if ok {
		return st.Code().String()
	}

	return codes.Unknown.String()
}

// https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/
func parseFullMethodName(full string) (string, string) {
	if len(full) == 0 {
		return strUnknown, strUnknown
	}
	trimmed := full
	if full[0] == '/' {
		trimmed = full[1:]
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return strUnknown, strUnknown
	}
	return parts[0], parts[1]
}

func (h *GRPCStatsHandler) HandleConn(ctx context.Context, connStats stats.ConnStats) {
	remoteAddr := ""
	if val, ok := ctx.Value(ctxKeyRemoteAddr).(string); ok {
		remoteAddr = val
	}

	host, port, err := net.SplitHostPort(remoteAddr)
	// remoteAddr is already tagged.
	clientIP := remoteAddr
	clientPort := strUnknown
	if err == nil {
		clientIP = host
		clientPort = port
	}

	switch connStats.(type) {
	case *stats.ConnBegin:
		h.activeConn.WithLabelValues().Inc()
		h.connTotalCount.WithLabelValues().Inc()
		slog.Info("[unisondb.grpc]",
			slog.Group("client",
				slog.Group("address", slog.String("ip", clientIP)),
				slog.String("port", clientPort)),
			slog.String("event_type", "grpc.connection.established"))

	case *stats.ConnEnd:
		h.activeConn.WithLabelValues().Dec()
		if startTime := ctx.Value(ctxKeyConnStartTime); startTime != nil {
			duration := time.Since(startTime.(time.Time))
			h.connDuration.WithLabelValues().Observe(duration.Seconds())
			slog.Info("[unisondb.grpc]", slog.Group("client",
				slog.Group("address", slog.String("ip", clientIP)),
				slog.String("port", clientPort)),
				slog.String("duration", humanizeDuration(duration)),
				slog.String("event_type", "grpc.connection.closed"))
		}
	}
}

// TagConn tags the connection object with the remoteAddr string if present.
func (h *GRPCStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	remoteAddr := strUnknown
	if info.RemoteAddr != nil {
		remoteAddr = info.RemoteAddr.String()
	}

	ctx = context.WithValue(ctx, ctxKeyRemoteAddr, remoteAddr)
	ctx = context.WithValue(ctx, ctxKeyConnStartTime, time.Now())
	return ctx
}

type streamKey struct {
	service string
	method  string
	bucket  string
	stream  string
}

func (h *GRPCStatsHandler) UpdateStreamAgeBuckets() {
	counts := make(map[streamKey]int)
	now := time.Now()

	h.activeStreamsMap.Range(func(key, value interface{}) bool {
		si, ok := value.(streamInfo)
		if !ok {
			return true
		}
		bucket := durationBucket(now.Sub(si.start))
		sk := streamKey{
			service: si.service,
			method:  si.method,
			bucket:  bucket,
			stream:  si.streamType,
		}
		counts[sk]++
		return true
	})

	// make sure to reset all the previous metrics.
	h.streamAgeBucket.Reset()
	for key, val := range counts {
		h.streamAgeBucket.WithLabelValues(key.service, key.method, key.stream, key.bucket).Set(float64(val))
	}
}

func durationBucket(d time.Duration) string {
	switch {
	case d < time.Second:
		return "0s_1s"
	case d < 10*time.Second:
		return "1s_10s"
	case d < 30*time.Second:
		return "10s_30s"
	case d < time.Minute:
		return "30s_1m"
	case d < 10*time.Minute:
		return "1m_10m"
	case d < 15*time.Minute:
		return "10m_15m"
	case d < 30*time.Minute:
		return "10m_30m"
	case d < time.Hour:
		return "30m_1h"
	case d < 6*time.Hour:
		return "1h_6h"
	case d < 12*time.Hour:
		return "6h_12h"
	case d < 24*time.Hour:
		return "12h_24h"
	default:
		return "24h_over"
	}
}

func humanizeDuration(d time.Duration) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		return d.String()
	}
	return s
}
