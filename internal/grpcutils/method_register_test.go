package grpcutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestRegisterGRPCSMethods(t *testing.T) {
	server := grpc.NewServer()

	// Register the gRPC health service
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)

	methodTypes := RegisterGRPCSMethods(server)

	expected := map[string]string{
		"/grpc.health.v1.Health/Check": "unary",
		"/grpc.health.v1.Health/Watch": "server_stream",
	}
	assert.Equal(t, expected, methodTypes)
}
