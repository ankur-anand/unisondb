package relayer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	promNamespace = "unisondb"
	promSubsystem = "relayer"
)

type ErrorKind int

const (
	Transient ErrorKind = iota
	Permanent
)

var (
	TransientErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubsystem,
			Name:      "retry_transient_errors_total",
			Help:      "Total transient retryable errors",
		},
		[]string{"namespace"},
	)

	PermanentErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Subsystem: promSubsystem,
			Name:      "retry_permanent_errors_total",
			Help:      "Total permanent non-retryable errors",
		},
		[]string{"namespace"},
	)

	LastRetryDuration = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: promNamespace,
			Subsystem: promSubsystem,
			Name:      "retry_last_duration_seconds",
			Help:      "Duration of last operation before retry or success",
		},
		[]string{"namespace"},
	)
)

func DefaultClassifier(err error) ErrorKind {
	if err == nil {
		return Transient
	}

	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
			return Transient
		case codes.OutOfRange:
			return Permanent
		default:
			return Permanent
		}
	}

	// fallback for plain errors
	if strings.Contains(err.Error(), "invalid") ||
		strings.Contains(err.Error(), "denied") ||
		strings.Contains(err.Error(), "truncated") {
		return Permanent
	}
	return Transient
}

// LoopOptions defines config for a long-running retry loop.
type LoopOptions struct {
	Namespace      string
	Run            func(context.Context) error
	Limiter        *rate.Limiter
	Classify       func(error) ErrorKind
	OnPermanentErr func(error)
}

// RunLoop executes a long-running retry loop with backoff and budgeting.
func RunLoop(ctx context.Context, opts LoopOptions) {
	bo := backoff.NewExponentialBackOff()

	for {
		select {
		case <-ctx.Done():
			slog.Debug("[unisondb.relayer] Retry loop cancelled", "namespace", opts.Namespace)
			return
		default:
		}

		if err := opts.Limiter.Wait(ctx); err != nil {
			if !errors.Is(context.Cause(ctx), context.Canceled) {
				slog.Warn("[unisondb.relayer] Rate limit blocked retry",
					"namespace", opts.Namespace, "err", err)
			}
			return
		}

		start := time.Now()
		err := opts.Run(ctx)
		duration := time.Since(start).Seconds()
		LastRetryDuration.WithLabelValues(opts.Namespace).Set(duration)

		if err == nil {
			return
		}

		if ctx.Err() != nil {
			return
		}

		switch opts.Classify(err) {
		case Permanent:
			PermanentErrors.WithLabelValues(opts.Namespace).Inc()
			slog.Error("[unisondb.relayer] Permanent error, stopping retries",
				"namespace", opts.Namespace,
				"error", err)
			if opts.OnPermanentErr != nil {
				opts.OnPermanentErr(err)
			}
			return

		case Transient:
			TransientErrors.WithLabelValues(opts.Namespace).Inc()
			slog.Error("[unisondb.relayer] Transient error encountered",
				"namespace", opts.Namespace,
				"error", err)
		}

		wait := bo.NextBackOff()
		// reset if the operation lasted longer than the backoff delay
		if duration > wait.Seconds() {
			slog.Debug("[unisondb.relayer] Resetting backoff after long successful run",
				"namespace", opts.Namespace,
				"runDuration", duration,
				"lastBackoff", wait,
			)
			bo.Reset()
			// we continue here as the service was healthy for long duration of time
			// and we should not slow down the recovery process.
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
