package grpcutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestRegisterGRPCSMethods(t *testing.T) {
	methodTypes := RegisterGRPCSMethods(healthpb.Health_ServiceDesc)
	
	expected := map[string]string{
		"/grpc.health.v1.Health/Check": "unary",
		"/grpc.health.v1.Health/Watch": "server_stream",
	}

	assert.Equal(t, expected, methodTypes)
}
