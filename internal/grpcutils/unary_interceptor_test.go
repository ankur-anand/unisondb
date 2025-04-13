package grpcutils_test

import (
	"context"
	"testing"

	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRequestIDUnaryInterceptor(t *testing.T) {
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Unary"}

	var called bool
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		md, ok := metadata.FromOutgoingContext(ctx)
		assert.True(t, ok)
		assert.NotEqual(t, len(md["x-request-id"]), 0)
		return "ok", nil
	}

	resp, err := grpcutils.RequestIDUnaryInterceptor(ctx, nil, info, handler)
	assert.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "ok", resp)
}

func TestRequireNamespaceUnaryInterceptor_Success(t *testing.T) {
	incoming := metadata.Pairs("x-namespace", "tenant-a")
	ctx := metadata.NewIncomingContext(context.Background(), incoming)
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Unary"}

	var called bool
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		md, _ := metadata.FromOutgoingContext(ctx)
		assert.NotEqual(t, len(md["x-namespace"]), 0)
		assert.Equal(t, "tenant-a", md["x-namespace"][0])
		return "ok", nil
	}

	resp, err := grpcutils.RequireNamespaceUnaryInterceptor(ctx, nil, info, handler)
	assert.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "ok", resp)
}

func TestRequireNamespaceUnaryInterceptor_Missing(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Unary"}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		t.Fatal("handler should not be called")
		return nil, nil
	}

	resp, err := grpcutils.RequireNamespaceUnaryInterceptor(ctx, nil, info, handler)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestMethodUnaryInterceptor(t *testing.T) {
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

	var called bool
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		md, _ := metadata.FromOutgoingContext(ctx)
		assert.NotEqual(t, len(md["x-method"]), 0)
		assert.Equal(t, "/test.Service/Method", md["x-method"][0])
		return "ok", nil
	}

	resp, err := grpcutils.MethodUnaryInterceptor(ctx, nil, info, handler)
	assert.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "ok", resp)
}
