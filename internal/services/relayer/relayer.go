package relayer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

var (
	ErrLSNLagThresholdExceeded = errors.New("LSN lag threshold exceeded")
	// TODO: Refactor this to return the name should be coming from the Streamer Interface itself.
	defaultStreamerLabel = "grpc"
)

var (
	lsnLagGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "wal_lsn_lag",
		Help:      "Difference in LSN between upstream and local WAL replica",
	}, []string{"namespace"})

	lsnLagThresholdGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "wal_lsn_lag_threshold",
		Help:      "Configured LSN lag threshold for the WAL relayer per namespace",
	}, []string{"namespace"})

	rateLimiterWaitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "rate_limiter_wait_duration_seconds",
		Help:      "Time spent waiting for rate limiter tokens.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"streamer"})

	rateLimiterErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "rate_limiter_errors_total",
		Help:      "Total number of rate limiter errors.",
	}, []string{"streamer"})

	rateLimiterSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "rate_limiter_success_total",
		Help:      "Total number of successful rate limiter grants.",
	}, []string{"streamer"})
)

type Streamer interface {
	GetLatestLSN(ctx context.Context) (uint64, error)
	StreamWAL(ctx context.Context) error
}

// WalIO defines the interface for writing Write-Ahead Log (WAL) records.
type WalIO interface {
	Write(data *v1.WALRecord) error
	WriteBatch(records []*v1.WALRecord) error
}

type walIOHandler struct {
	replica *dbkernel.ReplicaWALHandler
}

func (w walIOHandler) Write(data *v1.WALRecord) error {
	return w.replica.ApplyRecordsLSNOnly([][]byte{data.Record})
}

func (w walIOHandler) WriteBatch(records []*v1.WALRecord) error {
	if len(records) == 0 {
		return nil
	}

	encodedWals := make([][]byte, len(records))
	for i, record := range records {
		encodedWals[i] = record.Record
	}

	return w.replica.ApplyRecordsLSNOnly(encodedWals)
}

// RateLimitedWalIO wraps WalIO with a token bucket rate limiter.
type RateLimitedWalIO struct {
	// this should be the parent ctx that controls the entire program flow.
	ctx        context.Context
	underlying WalIO
	limiter    *rate.Limiter
}

// NewRateLimitedWalIO constructs a RateLimitedWalIO.
func NewRateLimitedWalIO(ctx context.Context, w WalIO, limiter *rate.Limiter) *RateLimitedWalIO {
	return &RateLimitedWalIO{
		ctx:        ctx,
		underlying: w,
		limiter:    limiter,
	}
}

// Write applies rate limiting before delegating to the underlying WalIO.
func (r *RateLimitedWalIO) Write(data *v1.WALRecord) error {
	start := time.Now()
	err := r.limiter.Wait(r.ctx)
	duration := time.Since(start)
	rateLimiterWaitDuration.WithLabelValues(defaultStreamerLabel).Observe(duration.Seconds())
	if err != nil {
		rateLimiterErrors.WithLabelValues(defaultStreamerLabel).Inc()
		return fmt.Errorf("rate limit exceeded: %w", err)
	}
	rateLimiterSuccess.WithLabelValues(defaultStreamerLabel).Inc()

	return r.underlying.Write(data)
}

// WriteBatch applies rate limiting before delegating to the underlying WalIO batch write.
func (r *RateLimitedWalIO) WriteBatch(records []*v1.WALRecord) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	// Reserve N tokens for N records
	err := r.limiter.WaitN(r.ctx, len(records))
	duration := time.Since(start)
	rateLimiterWaitDuration.WithLabelValues(defaultStreamerLabel).Observe(duration.Seconds())
	if err != nil {
		rateLimiterErrors.WithLabelValues(defaultStreamerLabel).Inc()
		return fmt.Errorf("rate limit exceeded: %w", err)
	}
	rateLimiterSuccess.WithLabelValues(defaultStreamerLabel).Inc()

	return r.underlying.WriteBatch(records)
}

// Relayer relays WAL record from the upstream over the provided grpc connection for the given namespace.
type Relayer struct {
	namespace string
	engine    *dbkernel.Engine
	// TODO: refactor, relayer should be independent of this?
	grpcConn        *grpc.ClientConn
	client          Streamer
	lsnLagThreshold int

	// protect duplicate start.
	started        atomic.Bool
	logger         *slog.Logger
	startSegmentID int
	startOffset    *dbkernel.Offset
	walIOHandler   WalIO

	offsetMonitorInterval time.Duration
}

// NewRelayer returns an initialized Relayer instance.
func NewRelayer(engine *dbkernel.Engine,
	namespace string,
	grpcConn *grpc.ClientConn,
	lsnLagThreshold int,
	log *slog.Logger) *Relayer {
	handler := dbkernel.NewReplicaWALHandler(engine)

	currentOffset := engine.CurrentOffset()
	segmentID := 0
	if currentOffset != nil {
		segmentID = int(currentOffset.SegmentID)
	}

	currentLSN := engine.OpsReceivedCount()

	walHandler := walIOHandler{replica: handler}
	client := streamer.NewGrpcStreamerClient(grpcConn, namespace,
		walHandler,
		currentLSN)

	lsnLagThresholdGauge.WithLabelValues(namespace).Set(float64(lsnLagThreshold))

	log.Info("[unisondb.relayer]",
		slog.String("event_type", "relayer.initialized"),
		slog.String("namespace", namespace),
		slog.Uint64("start_lsn", currentLSN),
	)

	return &Relayer{
		engine:          engine,
		namespace:       namespace,
		grpcConn:        grpcConn,
		lsnLagThreshold: lsnLagThreshold,
		logger:          log,
		startSegmentID:  segmentID,
		client:          client,
		startOffset:     currentOffset,
		walIOHandler:    walHandler,
	}
}

// CurrentWalIO returns the currently configured WalIO implementation for the Relayer.
//
//nolint:ireturn
func (r *Relayer) CurrentWalIO() WalIO {
	return r.walIOHandler
}

// EnableRateLimitedWalIO configures the Relayer to apply a token bucket rate limiter
// on WAL writes. This helps control replication throughput and prevents excessive
// memory or CPU usage by slowing down WAL processing at the consumer side.
// Must be called before calling the StartRelay else will panic.
// We are accepting the concrete implementation as that's what the function name is.
func (r *Relayer) EnableRateLimitedWalIO(walIO *RateLimitedWalIO) {
	if walIO == nil {
		return
	}

	if r.started.Load() {
		panic("cannot enable rate limiter after relay has started for namespace: " + r.namespace)
	}

	r.walIOHandler = walIO
	currentLSN := r.engine.OpsReceivedCount()
	r.client = streamer.NewGrpcStreamerClient(r.grpcConn, r.namespace, walIO, currentLSN)
	r.logger.Info("[unisondb.relayer]",
		slog.String("event_type", "relayer.rate_limiter.enabled"),
		slog.String("namespace", r.namespace),
	)
}

func (r *Relayer) StartRelay(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return fmt.Errorf("StartRelay already running for namespace %s", r.namespace)
	}

	defer r.started.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remoteLSN, err := r.client.GetLatestLSN(ctx)
	if err != nil {
		return err
	}

	localLSN := r.engine.OpsReceivedCount()

	go r.backgroundMonitorOffset(ctx)
	r.logger.Info("[unisondb.relayer]",
		slog.String("event_type", "relayer.relay.started"),
		slog.String("namespace", r.namespace),
		slog.Uint64("remote_lsn", remoteLSN),
		slog.Uint64("local_lsn", localLSN),
	)
	return r.client.StreamWAL(ctx)
}

func (r *Relayer) backgroundMonitorOffset(ctx context.Context) {
	if r.offsetMonitorInterval == 0 {
		r.offsetMonitorInterval = 1 * time.Minute
	}
	ticker := time.NewTicker(r.offsetMonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.monitor(ctx)
		}
	}
}

func (r *Relayer) monitor(ctx context.Context) {
	localLSN := r.engine.OpsReceivedCount()
	remoteLSN, err := r.client.GetLatestLSN(ctx)
	if err != nil {
		r.logger.Error("[unisondb.relayer] error getting latest LSN",
			"event_type", "error",
			"error", err,
			"namespace", r.namespace)
		return
	}

	lsnLag := int64(remoteLSN) - int64(localLSN)
	if lsnLag < 0 {
		lsnLag = 0
	}
	lsnLagGauge.WithLabelValues(r.namespace).Set(float64(lsnLag))

	if lsnLag > int64(r.lsnLagThreshold) {
		r.logger.Warn("[unisondb.relayer]",
			slog.String("event_type", "lsn.lag.threshold.exceeded"),
			slog.String("namespace", r.namespace),
			slog.Uint64("remote_lsn", remoteLSN),
			slog.Uint64("local_lsn", localLSN),
			slog.Int64("lsn_lag", lsnLag),
		)
	}
}
