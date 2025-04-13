package grpcutils

import (
	"context"
	"log/slog"
	"maps"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var defaultLogRequestMethodDisabled = map[string]bool{
	"/grpc.health.v1.Health/Check": false,
	"/grpc.health.v1.Health/Watch": false,
}

var slowConsumerCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "grpc_slow_consumer_total",
		Help: "Count of slow consumer messages per gRPC method",
	},
	[]string{"grpc_service", "grpc_method", "stream_type"},
)

// Interceptor is a StateFull Interceptor for GRPC.
type Interceptor struct {
	// LogRequestAllowedMethod stores all the method for which logs are allowed.
	// Key is a map of service name + method
	LogRequestEnabledMethod map[string]bool
	logger                  *slog.Logger
}

// NewStatefulInterceptor returns an initialized Interceptor.
func NewStatefulInterceptor(logger *slog.Logger, logRequestEnabledMethod map[string]bool) *Interceptor {
	logMap := maps.Clone(logRequestEnabledMethod)
	maps.Copy(logMap, defaultLogRequestMethodDisabled)
	return &Interceptor{
		LogRequestEnabledMethod: logMap,
		logger:                  logger,
	}
}

// TelemetryStreamInterceptor logs gRPC requests, responses, and status codes and basic metrics for streaming API.
func (i *Interceptor) TelemetryStreamInterceptor(srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) error {
	logEnabled := i.LogRequestEnabledMethod[info.FullMethod]

	startTime := time.Now()
	requestID := extractRequestID(ss.Context())
	ns := GetNamespace(ss.Context())
	service, method := parseFullMethodName(info.FullMethod)

	if logEnabled {
		i.logger.Info("[unisondb.grpc]",
			slog.String("event_type", "rpc.stream.started"),
			slog.Group("grpc",
				slog.String("method", method),
				slog.String("service", service),
				slog.String("grpc_type", grpcStreamType(info))),
			slog.Group("request", slog.String("id", requestID),
				slog.String("client", peerAddress(ss.Context())),
				slog.String("namespace", ns),
				slog.String("deadline", getDeadline(ss.Context()))),
		)
	}

	// handler
	err := handler(srv, ss)

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	grpcStatusCode := status.Code(err)

	// Log completion.
	if err != nil {
		i.logger.Error("[unisondb.grpc]",
			slog.String("event_type", "rpc.stream.failed"),
			slog.Group("grpc",
				slog.String("method", method),
				slog.String("service", service),
				slog.String("grpc_type", grpcStreamType(info))),
			slog.Group("request", slog.String("id", requestID),
				slog.String("client", peerAddress(ss.Context())),
				slog.String("namespace", ns),
				slog.String("deadline", getDeadline(ss.Context()))),
			slog.Group("response",
				slog.String("status_code", grpcStatusCode.String()),
				slog.String("duration", humanizeDuration(duration)),
				slog.String("error", err.Error())),
		)
	} else if logEnabled {
		i.logger.Info("[unisondb.grpc]",
			slog.String("event_type", "rpc.stream.completed"),
			slog.Group("grpc",
				slog.String("method", method),
				slog.String("service", service),
				slog.String("grpc_type", grpcStreamType(info))),
			slog.Group("request", slog.String("id", requestID),
				slog.String("client", peerAddress(ss.Context())),
				slog.String("namespace", ns),
				slog.String("deadline", getDeadline(ss.Context()))),
			slog.Group("response",
				slog.String("status_code", grpcStatusCode.String()),
				slog.String("duration", humanizeDuration(duration))),
		)
	}

	return err
}

func (i *Interceptor) TelemetryUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	logEnabled := i.LogRequestEnabledMethod[info.FullMethod]

	startTime := time.Now()
	requestID := extractRequestID(ctx)
	ns := GetNamespace(ctx)
	service, method := parseFullMethodName(info.FullMethod)

	if logEnabled {
		i.logger.Info("[unisondb.grpc]",
			slog.String("event_type", "rpc.request.started"),
			slog.Group("grpc",
				slog.String("method", method),
				slog.String("service", service),
				slog.String("grpc_type", unary)),
			slog.Group("request",
				slog.String("id", requestID),
				slog.String("client", peerAddress(ctx)),
				slog.String("namespace", ns),
				slog.String("deadline", getDeadline(ctx))),
		)
	}

	resp, err := handler(ctx, req)

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	grpcStatusCode := status.Code(err)

	// Log completion.
	if err != nil {
		i.logger.Error("[unisondb.grpc]",
			slog.String("event_type", "rpc.request.failed"),
			slog.Group("grpc",
				slog.String("method", method),
				slog.String("service", service),
				slog.String("grpc_type", unary)),
			slog.Group("request", slog.String("id", requestID),
				slog.String("client", peerAddress(ctx)),
				slog.String("namespace", ns),
				slog.String("deadline", getDeadline(ctx))),
			slog.Group("response",
				slog.String("status_code", grpcStatusCode.String()),
				slog.String("duration", humanizeDuration(duration)),
				slog.String("error", err.Error())),
		)
	} else if logEnabled {
		i.logger.Info("[unisondb.grpc]",
			slog.String("event_type", "rpc.request.completed"),
			slog.Group("grpc",
				slog.String("method", method),
				slog.String("service", service),
				slog.String("grpc_type", unary)),
			slog.Group("request",
				slog.String("id", requestID),
				slog.String("client", peerAddress(ctx)),
				slog.String("namespace", ns),
				slog.String("deadline", getDeadline(ctx))),
			slog.Group("response",
				slog.String("status_code", grpcStatusCode.String()),
				slog.String("duration", humanizeDuration(duration))),
		)
	}

	return resp, err
}
