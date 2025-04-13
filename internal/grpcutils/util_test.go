package grpcutils_test

import (
	"context"
	"testing"

	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestGetRequestID(t *testing.T) {
	t.Run("returns x-request-id when present", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "req-123"))
		got := grpcutils.GetRequestID(ctx)
		assert.Equal(t, "req-123", got)
	})

	t.Run("returns empty string when missing", func(t *testing.T) {
		ctx := context.Background()
		got := grpcutils.GetRequestID(ctx)
		assert.Equal(t, "", got)
	})
}

func TestGetMethod(t *testing.T) {
	t.Run("returns x-method when present", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-method", "/test.Service/Method"))
		method := grpcutils.GetMethod(ctx)
		assert.Equal(t, "/test.Service/Method", method)
	})

	t.Run("returns empty string when missing", func(t *testing.T) {
		ctx := context.Background()
		method := grpcutils.GetMethod(ctx)
		assert.Equal(t, "", method)
	})
}

func TestGetNamespace(t *testing.T) {
	t.Run("returns x-namespace when present", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-namespace", "tenant-a"))
		ns := grpcutils.GetNamespace(ctx)
		assert.Equal(t, "tenant-a", ns)
	})

	t.Run("returns empty string when missing", func(t *testing.T) {
		ctx := context.Background()
		ns := grpcutils.GetNamespace(ctx)
		assert.Equal(t, "", ns)
	})
}
