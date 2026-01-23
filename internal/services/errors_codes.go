package services

import (
	"errors"
	"log/slog"

	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrMissingNamespace           = errors.New("missing required parameter: namespace")
	ErrNamespaceNotExists         = errors.New("namespace is not found on the current server")
	ErrInvalidMetadata            = errors.New("bad metadata in request")
	ErrMissingNamespaceInMetadata = errors.New("missing required metadata: x-namespace")
	ErrStreamTimeout              = errors.New("stream timeout: no records received within dynamic threshold seconds")
	ErrKeyNotFound                = status.Error(codes.NotFound, "key not found")
	ErrPutChunkPrecondition       = errors.New("invalid sequence: StartMarker must be sent before sending chunks or calling CommitMarker")
	ErrPutChunkCheckSumMismatch   = errors.New("invalid checksum: checksum mismatch")
	ErrPutChunkAlreadyCommited    = errors.New("put chunk stream already commited")
	ErrClientMaxRetriesExceeded   = errors.New("max retries exceeded")
	ErrLSNNotYetAvailable         = errors.New("LSN not yet available")
	ErrLSNTruncated               = errors.New("LSN truncated")
)

// ToGRPCError Convert business error to gRPC error.
// Custom types to avoid ordering issue while calling the function.
func ToGRPCError(namespace string, reqID grpcutils.RequestID, method grpcutils.Method, err error) error {
	if isLSNNotYetAvailable(err) {
		return status.Error(codes.Unavailable, ErrLSNNotYetAvailable.Error())
	}
	if isLSNTruncated(err) {
		return status.Error(codes.OutOfRange, ErrLSNTruncated.Error())
	}

	switch {
	case errors.Is(err, ErrMissingNamespace):
		return status.Error(codes.InvalidArgument, ErrMissingNamespace.Error())
	case errors.Is(err, ErrNamespaceNotExists):
		return status.Error(codes.NotFound, ErrNamespaceNotExists.Error())
	case errors.Is(err, ErrInvalidMetadata):
		return status.Error(codes.InvalidArgument, ErrInvalidMetadata.Error())
	case errors.Is(err, ErrMissingNamespaceInMetadata):
		return status.Error(codes.InvalidArgument, ErrMissingNamespaceInMetadata.Error())
	case errors.Is(err, ErrStreamTimeout):
		return status.Error(codes.Unavailable, ErrStreamTimeout.Error())
	case errors.Is(err, ErrPutChunkPrecondition):
		return status.Error(codes.FailedPrecondition, ErrPutChunkPrecondition.Error())
	case errors.Is(err, ErrPutChunkCheckSumMismatch):
		return status.Error(codes.DataLoss, ErrPutChunkCheckSumMismatch.Error())
	case errors.Is(err, ErrPutChunkAlreadyCommited):
		return status.Error(codes.Aborted, ErrPutChunkAlreadyCommited.Error())
	default:
		slog.Error("[GRPC] service error", "error", err,
			"method", method,
			"request_id", reqID,
			"namespace", namespace)
		return status.Errorf(codes.Internal, "internal server error")
	}
}

func isLSNNotYetAvailable(err error) bool {
	if errors.Is(err, ErrLSNNotYetAvailable) {
		return true
	}
	var notYet *walfs.LSNNotYetAvailableError
	return errors.As(err, &notYet)
}

func isLSNTruncated(err error) bool {
	if errors.Is(err, ErrLSNTruncated) {
		return true
	}
	var truncated *walfs.LSNTruncatedError
	return errors.As(err, &truncated)
}
