package services_test

import (
	"testing"

	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCError_LSNMappings(t *testing.T) {
	reqID := grpcutils.RequestID("req-1")
	method := grpcutils.Method("StreamWalRecords")

	err := services.ToGRPCError("ns", reqID, method, services.ErrLSNNotYetAvailable)
	sErr := status.Convert(err)
	assert.Equal(t, codes.Unavailable, sErr.Code())
	assert.Equal(t, services.ErrLSNNotYetAvailable.Error(), sErr.Message())

	err = services.ToGRPCError("ns", reqID, method, services.ErrLSNTruncated)
	sErr = status.Convert(err)
	assert.Equal(t, codes.OutOfRange, sErr.Code())
	assert.Equal(t, services.ErrLSNTruncated.Error(), sErr.Message())

	err = services.ToGRPCError("ns", reqID, method, &walfs.LSNNotYetAvailableError{
		RequestedLSN:  10,
		CurrentMaxLSN: 5,
	})
	sErr = status.Convert(err)
	assert.Equal(t, codes.Unavailable, sErr.Code())
	assert.Equal(t, services.ErrLSNNotYetAvailable.Error(), sErr.Message())

	err = services.ToGRPCError("ns", reqID, method, &walfs.LSNTruncatedError{
		RequestedLSN:      2,
		FirstAvailableLSN: 5,
	})
	sErr = status.Convert(err)
	assert.Equal(t, codes.OutOfRange, sErr.Code())
	assert.Equal(t, services.ErrLSNTruncated.Error(), sErr.Message())
}
