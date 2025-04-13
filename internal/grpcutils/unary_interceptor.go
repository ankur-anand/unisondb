package grpcutils

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func RequestIDUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	requestID := extractRequestID(ctx)
	newCtx := metadata.AppendToOutgoingContext(ctx, "x-request-id", requestID)
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
