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

type mockServerStream struct {
	grpc.ServerStream
	ctx    context.Context
	header metadata.MD
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

func (m *mockServerStream) SetHeader(md metadata.MD) error {
	m.header = md
	return nil
}

func TestRequestIDStreamInterceptor(t *testing.T) {
	md := metadata.New(nil)
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := &mockServerStream{ctx: ctx}

	var handlerCalled bool
	handler := func(srv interface{}, ss grpc.ServerStream) error {
		handlerCalled = true
		md, _ := metadata.FromOutgoingContext(ss.Context())
		assert.NotEqual(t, len(md["x-request-id"]), 0)

		return nil
	}

	err := grpcutils.RequestIDStreamInterceptor(nil, stream, &grpc.StreamServerInfo{FullMethod: "/Test"}, handler)
	assert.NoError(t, err)
	assert.True(t, handlerCalled, "handler was not called")
	assert.NotEqual(t, len(stream.header["x-request-id"]), 0)
}

func TestRequireNamespaceInterceptor_Success(t *testing.T) {
	md := metadata.Pairs("x-namespace", "tenant-a")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	stream := &mockServerStream{ctx: ctx}

	var handlerCalled bool
	handler := func(srv interface{}, ss grpc.ServerStream) error {
		handlerCalled = true
		md, _ := metadata.FromOutgoingContext(ss.Context())
		assert.NotEqual(t, len(md["x-namespace"]), 0)
		assert.Equal(t, md["x-namespace"][0], "tenant-a")
		return nil
	}

	err := grpcutils.RequireNamespaceInterceptor(nil, stream, &grpc.StreamServerInfo{FullMethod: "/Test"}, handler)
	assert.NoError(t, err)
	assert.True(t, handlerCalled, "handler was not called")
}

func TestRequireNamespaceInterceptor_MissingNamespace(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	stream := &mockServerStream{ctx: ctx}

	handler := func(srv interface{}, ss grpc.ServerStream) error {
		t.Error("handler should not be called when namespace is missing")
		return nil
	}

	err := grpcutils.RequireNamespaceInterceptor(nil, stream, &grpc.StreamServerInfo{}, handler)
	assert.Error(t, err)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestMethodInterceptor(t *testing.T) {
	ctx := context.Background()
	stream := &mockServerStream{ctx: ctx}

	var handlerCalled bool
	handler := func(srv interface{}, ss grpc.ServerStream) error {
		handlerCalled = true
		md, _ := metadata.FromOutgoingContext(ss.Context())
		assert.NotEqual(t, len(md["x-method"]), 0)
		assert.Equal(t, md["x-method"][0], "/test.Service/Method")
		return nil
	}

	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/Method"}

	err := grpcutils.MethodInterceptor(nil, stream, info, handler)
	assert.NoError(t, err)
	assert.True(t, handlerCalled, "handler was not called")
}
