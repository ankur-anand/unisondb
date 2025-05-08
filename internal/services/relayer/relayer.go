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
	"google.golang.org/grpc"
)

type Streamer interface {
	GetLatestOffset(ctx context.Context) (*dbkernel.Offset, error)
	StreamWAL(ctx context.Context) error
}

var (
	segmentLagGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "wal_segment_lag",
		Help:      "Difference in segment IDs between upstream and local WAL replica",
	}, []string{"namespace"})

	segmentLagThresholdGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: promNamespace,
		Subsystem: promSubsystem,
		Name:      "wal_segment_lag_threshold",
		Help:      "Configured segment lag threshold for the WAL relayer per namespace",
	}, []string{"namespace"})
)

type walIOHandler struct {
	replica *dbkernel.ReplicaWALHandler
}

func (w walIOHandler) Write(data *v1.WALRecord) error {
	return w.replica.ApplyRecord(data.Record, data.Offset)
}

var (
	ErrSegmentLagThresholdExceeded = errors.New("segment lag threshold exceeded")
)

// Relayer relays WAL record from the upstream over the provided grpc connection for the given namespace.
type Relayer struct {
	namespace           string
	engine              *dbkernel.Engine
	grpcConn            *grpc.ClientConn
	client              Streamer
	segmentLagThreshold int

	// protect duplicate start.
	started        atomic.Bool
	logger         *slog.Logger
	startSegmentID int
	startOffset    *dbkernel.Offset
	walIOHandler   walIOHandler

	offsetMonitorInterval time.Duration
}

// NewRelayer returns an initialized Relayer instance.
func NewRelayer(engine *dbkernel.Engine,
	namespace string,
	grpcConn *grpc.ClientConn,
	segmentLagThreshold int,
	log *slog.Logger) *Relayer {
	handler := dbkernel.NewReplicaWALHandler(engine)

	currentOffset := engine.CurrentOffset()
	segmentID := 0
	if currentOffset != nil {
		segmentID = int(currentOffset.SegmentID)
	}

	var currOffset []byte
	if currentOffset != nil {
		currOffset = currentOffset.Encode()
	}
	walHandler := walIOHandler{replica: handler}
	client := streamer.NewGrpcStreamerClient(grpcConn, namespace,
		walHandler,
		currOffset)

	segmentLagThresholdGauge.WithLabelValues(namespace).Set(float64(segmentLagThreshold))
	return &Relayer{
		engine:              engine,
		namespace:           namespace,
		grpcConn:            grpcConn,
		segmentLagThreshold: segmentLagThreshold,
		logger:              log,
		startSegmentID:      segmentID,
		client:              client,
		startOffset:         currentOffset,
		walIOHandler:        walHandler,
	}
}

// StartRelay starts the WAL replication Sync with the upstream over the provided grpc-connection,
// for the given namespace.
func (r *Relayer) StartRelay(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return fmt.Errorf("StartRelay already running for namespace %s", r.namespace)
	}

	defer r.started.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remoteOffset, err := r.client.GetLatestOffset(ctx)
	if err != nil {
		return err
	}

	if remoteOffset != nil && int(remoteOffset.SegmentID)-r.startSegmentID > r.segmentLagThreshold {
		r.logLag(remoteOffset, r.startOffset)
		return fmt.Errorf("%w %d", ErrSegmentLagThresholdExceeded, r.segmentLagThreshold)
	}
	remoteOffset = reAssignIfNil(remoteOffset)
	localOffset := reAssignIfNil(r.startOffset)

	go r.backgroundMonitorOffset(ctx)
	// start streaming
	r.logger.Info("[unisondb.relayer]",
		slog.String("event_type", "relayer.relay.started"),
		slog.String("namespace", r.namespace),
		slog.Group("offset",
			slog.Group("remote",
				slog.Int("segment_id", int(remoteOffset.SegmentID)),
				slog.Int("offset", int(remoteOffset.Offset)),
			),
			slog.Group("local",
				slog.Int("segment_id", int(localOffset.SegmentID)),
				slog.Int("offset", int(localOffset.Offset)),
			),
		),
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
	currentOffset := r.engine.CurrentOffset()
	remoteOffset, err := r.client.GetLatestOffset(ctx)
	if err != nil {
		r.logger.Error("[unisondb.relayer] error getting latest offset",
			"event_type", "error",
			"error", err,
			"namespace", r.namespace)
		return
	}

	segmentID := 0
	if currentOffset != nil {
		segmentID = int(currentOffset.SegmentID)
	}

	if remoteOffset != nil {
		segmentLag := int(remoteOffset.SegmentID) - segmentID
		segmentLagGauge.WithLabelValues(r.namespace).Set(float64(segmentLag))

		if segmentLag > r.segmentLagThreshold {
			r.logLag(remoteOffset, currentOffset)
		}
	}
}

func (r *Relayer) logLag(remote, local *dbkernel.Offset) {
	if remote == nil && local == nil {
		return
	}

	remote = reAssignIfNil(remote)
	local = reAssignIfNil(local)

	r.logger.Warn("[unisondb.relayer]",
		slog.String("event_type", "segment.lag.threshold.exceeded"),
		slog.String("namespace", r.namespace),
		slog.Group("offset",
			slog.Group("remote",
				slog.Int("segment_id", int(remote.SegmentID)),
				slog.Int("offset", int(remote.Offset)),
			),
			slog.Group("local",
				slog.Int("segment_id", int(local.SegmentID)),
				slog.Int("offset", int(local.Offset)),
			),
		),
	)
}

func reAssignIfNil(offset *dbkernel.Offset) *dbkernel.Offset {
	if offset == nil {
		return &dbkernel.Offset{}
	}
	return offset
}
