package relayer_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/internal/services/relayer"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRunLoop_Success(t *testing.T) {
	var called int32

	opts := relayer.LoopOptions{
		Namespace: "test_success",
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&called, 1)
			return nil
		},
		Limiter:  rate.NewLimiter(rate.Inf, 1),
		Classify: relayer.DefaultClassifier,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	relayer.RunLoop(ctx, opts)

	assert.Equal(t, int32(1), atomic.LoadInt32(&called))
}

func TestRunLoop_TransientRetry(t *testing.T) {
	var attempt int32

	opts := relayer.LoopOptions{
		Namespace: "test_transient",
		Run: func(ctx context.Context) error {
			n := atomic.AddInt32(&attempt, 1)
			if n < 3 {
				return errors.New("temporary failure")
			}
			return nil
		},
		Limiter:  rate.NewLimiter(rate.Every(10*time.Millisecond), 1),
		Classify: relayer.DefaultClassifier,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	relayer.RunLoop(ctx, opts)

	assert.GreaterOrEqual(t, atomic.LoadInt32(&attempt), int32(3))
}

func TestRunLoop_PermanentError(t *testing.T) {
	var attempt int32
	var permanentErr error
	errExpected := errors.New("denied by server")

	opts := relayer.LoopOptions{
		Namespace: "test_permanent",
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&attempt, 1)
			return errExpected
		},
		Limiter:  rate.NewLimiter(rate.Inf, 1),
		Classify: relayer.DefaultClassifier,
		OnPermanentErr: func(err error) {
			permanentErr = err
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	relayer.RunLoop(ctx, opts)

	assert.Equal(t, int32(1), atomic.LoadInt32(&attempt))
	assert.ErrorIs(t, errExpected, permanentErr)
}

func TestRunLoop_ContextCancelled(t *testing.T) {
	var called int32

	opts := relayer.LoopOptions{
		Namespace: "test_ctx_cancel",
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&called, 1)
			return errors.New("some transient error")
		},
		Limiter:  rate.NewLimiter(rate.Every(50*time.Millisecond), 1),
		Classify: relayer.DefaultClassifier,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	relayer.RunLoop(ctx, opts)

	assert.GreaterOrEqual(t, atomic.LoadInt32(&called), int32(1))
}

func TestDefaultClassifier(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected relayer.ErrorKind
	}{
		{
			name:     "nil error is transient",
			err:      nil,
			expected: relayer.Transient,
		},
		{
			name:     "gRPC Unavailable is transient",
			err:      status.Error(codes.Unavailable, "server down"),
			expected: relayer.Transient,
		},
		{
			name:     "gRPC DeadlineExceeded is transient",
			err:      status.Error(codes.DeadlineExceeded, "timeout"),
			expected: relayer.Transient,
		},
		{
			name:     "gRPC ResourceExhausted is transient",
			err:      status.Error(codes.ResourceExhausted, "too many requests"),
			expected: relayer.Transient,
		},
		{
			name:     "gRPC OutOfRange is permanent",
			err:      status.Error(codes.OutOfRange, "LSN truncated"),
			expected: relayer.Permanent,
		},
		{
			name:     "gRPC InvalidArgument is permanent",
			err:      status.Error(codes.InvalidArgument, "bad request"),
			expected: relayer.Permanent,
		},
		{
			name:     "error with 'invalid' in message is permanent",
			err:      errors.New("invalid config"),
			expected: relayer.Permanent,
		},
		{
			name:     "error with 'denied' in message is permanent",
			err:      errors.New("access denied"),
			expected: relayer.Permanent,
		},
		{
			name:     "error with 'truncated' in message is permanent",
			err:      errors.New("lsn truncated at 10"),
			expected: relayer.Permanent,
		},
		{
			name:     "generic error is transient",
			err:      errors.New("unexpected network drop"),
			expected: relayer.Transient,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := relayer.DefaultClassifier(tt.err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestRunLoop_ContextCancelledBeforeLoop(t *testing.T) {
	var called int32

	opts := relayer.LoopOptions{
		Namespace: "test_ctx_cancelled",
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&called, 1)
			return nil
		},
		Limiter:  rate.NewLimiter(rate.Inf, 1),
		Classify: relayer.DefaultClassifier,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately before calling RunLoop

	relayer.RunLoop(ctx, opts)

	// Run should never be called since context was already cancelled
	assert.Equal(t, int32(0), atomic.LoadInt32(&called))
}
