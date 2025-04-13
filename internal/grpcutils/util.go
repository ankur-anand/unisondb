package grpcutils

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

var (
	ErrMissingNamespaceInMetadata = errors.New("missing required metadata: x-namespace")
)

type RequestID string
type Method string

// GetRequestInfo extracts namespace, request ID, and method from the context.
func GetRequestInfo(ctx context.Context) (string, RequestID, Method) {
	return GetNamespace(ctx), RequestID(GetRequestID(ctx)), Method(GetMethod(ctx))
}

func GetMethod(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-method"); len(values) > 0 {
		return values[0]
	}
	return ""
}

func GetNamespace(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-namespace"); len(values) > 0 {
		return values[0]
	}
	return ""
}

func GetRequestID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get("x-request-id"); len(values) > 0 {
		return values[0]
	}
	return ""
}

// extractRequestID retrieves x-request-id or x-trace-id from gRPC metadata.
// If not present, it generates a new one.
func extractRequestID(ctx context.Context) string {
	// get trace from OpenTelemetry span
	if span := trace.SpanFromContext(ctx); span != nil {
		traceID := span.SpanContext().TraceID()
		if traceID.IsValid() {
			return traceID.String()
		}
	}

	// try fallback
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return generateNewRequestID()
	}

	if values := md.Get("x-request-id"); len(values) > 0 {
		return values[0]
	}

	if values := md.Get("x-trace-id"); len(values) > 0 {
		return values[0]
	}

	// If missing, generate a new request ID
	return generateNewRequestID()
}

// generateNewRequestID creates a new UUID-based request ID.
func generateNewRequestID() string {
	return uuid.New().String()
}

func grpcStreamType(info *grpc.StreamServerInfo) string {
	switch {
	case info.IsClientStream && info.IsServerStream:
		return bidiStream
	case info.IsServerStream:
		return serverStream
	case info.IsClientStream:
		return clientStream
	default:
		return unary
	}
}

func peerAddress(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

func getDeadline(ctx context.Context) string {
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline).String()
	}
	return "none"
}
