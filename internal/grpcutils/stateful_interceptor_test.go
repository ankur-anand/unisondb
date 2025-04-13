package grpcutils_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testMethod = "/test.Service/TestMethod"

func setupLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	return logger, &buf
}

func TestTelemetryUnaryInterceptor_Success(t *testing.T) {
	logger, logBuf := setupLogger()
	i := grpcutils.NewStatefulInterceptor(logger, map[string]bool{
		testMethod: true,
	})

	info := &grpc.UnaryServerInfo{
		FullMethod: testMethod,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	_, err := i.TelemetryUnaryInterceptor(context.Background(), nil, info, handler)
	assert.NoError(t, err)

	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "rpc.request.started")
	assert.Contains(t, logOutput, "rpc.request.completed")
}

func TestTelemetryUnaryInterceptor_Error(t *testing.T) {
	logger, logBuf := setupLogger()
	i := grpcutils.NewStatefulInterceptor(logger, map[string]bool{
		testMethod: true,
	})

	info := &grpc.UnaryServerInfo{
		FullMethod: testMethod,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, errors.New("boom")
	}

	_, err := i.TelemetryUnaryInterceptor(context.Background(), nil, info, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boom")

	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "rpc.request.failed")
	assert.Contains(t, logOutput, "boom")
}

func TestTelemetryStreamInterceptor(t *testing.T) {
	logger, logBuf := setupLogger()
	i := grpcutils.NewStatefulInterceptor(logger, map[string]bool{
		testMethod: true,
	})

	streamInfo := &grpc.StreamServerInfo{
		FullMethod:     testMethod,
		IsClientStream: true,
		IsServerStream: true,
	}
	stream := &mockServerStream{ctx: context.Background()}

	handler := func(srv interface{}, stream grpc.ServerStream) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	err := i.TelemetryStreamInterceptor(nil, stream, streamInfo, handler)
	assert.NoError(t, err)

	output := logBuf.String()
	assert.Contains(t, output, "rpc.stream.started")
	assert.Contains(t, output, "rpc.stream.completed")
}

func TestTelemetryStreamInterceptor_Error(t *testing.T) {
	logger, logBuf := setupLogger()
	i := grpcutils.NewStatefulInterceptor(logger, map[string]bool{
		testMethod: true,
	})

	streamInfo := &grpc.StreamServerInfo{
		FullMethod:     testMethod,
		IsClientStream: true,
		IsServerStream: true,
	}
	stream := &mockServerStream{ctx: context.Background()}

	handler := func(srv interface{}, stream grpc.ServerStream) error {
		time.Sleep(5 * time.Millisecond)
		return status.Errorf(codes.Internal, "simulated internal error")
	}

	err := i.TelemetryStreamInterceptor(nil, stream, streamInfo, handler)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "simulated internal error")

	output := logBuf.String()
	assert.Contains(t, output, "rpc.stream.failed")
	assert.Contains(t, output, "simulated internal error")
	assert.Contains(t, output, "status_code=Internal")
	assert.Contains(t, output, "status_code")
}

func TestTelemetryUnaryInterceptor_HealthCheck_NoLog(t *testing.T) {
	logger, logBuf := setupLogger()
	i := grpcutils.NewStatefulInterceptor(logger, map[string]bool{}) // no override

	info := &grpc.UnaryServerInfo{
		FullMethod: "/grpc.health.v1.Health/Check",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	_, err := i.TelemetryUnaryInterceptor(context.Background(), nil, info, handler)
	assert.NoError(t, err)
	assert.Len(t, logBuf.String(), 0)
}

func TestTelemetryStreamInterceptor_HealthWatch_NoLog(t *testing.T) {
	logger, logBuf := setupLogger()
	i := grpcutils.NewStatefulInterceptor(logger, map[string]bool{}) // no override

	streamInfo := &grpc.StreamServerInfo{
		FullMethod:     "/grpc.health.v1.Health/Watch",
		IsServerStream: true,
		IsClientStream: false,
	}
	stream := &mockServerStream{ctx: context.Background()}

	handler := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	err := i.TelemetryStreamInterceptor(nil, stream, streamInfo, handler)
	assert.NoError(t, err)
	assert.Len(t, logBuf.String(), 0)
}
