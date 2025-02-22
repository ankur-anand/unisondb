package middleware

import (
	"context"
	"expvar"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	totalActiveGrpcReq = expvar.NewInt("total_active_grpc_requests")
)

func RequestIDUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	requestID := extractRequestID(ctx)
	newCtx := metadata.AppendToOutgoingContext(ctx, "x-request-id", requestID)
	resp, err := handler(newCtx, req)
	return resp, err
}

func CorrelationIDUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	var correlationID string

	if ok {
		if values := md.Get("x-correlation-id"); len(values) > 0 {
			correlationID = values[0]
		}
		if correlationID == "" {
			if values := md.Get("x-request-id"); len(values) > 0 {
				correlationID = values[0]
			}
		}
	}

	newCtx := metadata.AppendToOutgoingContext(ctx, "x-correlation-id", correlationID)
	resp, err := handler(newCtx, req)
	return resp, err
}

func RequireNamespaceUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("x-namespace")) == 0 {
		return nil, status.Error(codes.InvalidArgument, ErrMissingNamespaceInMetadata.Error())
	}
	namespace := md.Get("x-namespace")[0]
	newCtx := metadata.AppendToOutgoingContext(ctx, "x-namespace", namespace)
	return handler(newCtx, req)
}

func MethodUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	newCtx := metadata.AppendToOutgoingContext(ctx, "x-method", info.FullMethod)
	return handler(newCtx, req)
}

func TelemetryUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	startTime := time.Now()
	totalActiveGrpcReq.Add(1)
	defer totalActiveGrpcReq.Add(-1)

	requestID := extractRequestID(ctx)
	ns := GetNamespace(ctx)

	resp, err := handler(ctx, req)

	duration := time.Since(startTime)
	st, _ := status.FromError(err)
	grpcStatus := st.Code().String()

	if err != nil {
		slog.Error("[GRPC] Request Failed", "method",
			info.FullMethod,
			"status", grpcStatus,
			"duration_sec", duration.Seconds(),
			"error", err.Error(), reqIDKey, requestID,
			"namespace", ns)
	} else {
		slog.Info("[GRPC] Request Completed",
			"method", info.FullMethod,
			"status", grpcStatus, "duration_sec",
			duration.Seconds(), reqIDKey, requestID,
			"namespace", ns)
	}

	return resp, err
}
