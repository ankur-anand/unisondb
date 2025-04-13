package grpcutils

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// wrappedServerStream wraps gRPC ServerStream to modify the context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// RequestIDStreamInterceptor ensures every streaming gRPC request has a request ID.
func RequestIDStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	requestID := extractRequestID(ss.Context())

	newCtx := metadata.AppendToOutgoingContext(ss.Context(), "x-request-id", requestID)
	wrappedStream := &wrappedServerStream{ServerStream: ss, ctx: newCtx}

	if err := ss.SetHeader(metadata.Pairs("x-request-id", requestID)); err != nil {
		return err
	}

	return handler(srv, wrappedStream)
}

func RequireNamespaceInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())

	if !ok || len(md.Get("x-namespace")) == 0 {
		return status.Error(codes.InvalidArgument, ErrMissingNamespaceInMetadata.Error())
	}

	namespace := md.Get("x-namespace")[0]

	ctx := metadata.AppendToOutgoingContext(ss.Context(), "x-namespace", namespace)
	wrappedStream := &wrappedServerStream{ServerStream: ss, ctx: ctx}

	return handler(srv, wrappedStream)
}

func MethodInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := metadata.AppendToOutgoingContext(ss.Context(), "x-method", info.FullMethod)
	wrappedStream := &wrappedServerStream{ServerStream: ss, ctx: ctx}

	return handler(srv, wrappedStream)
}
