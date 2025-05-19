package streamer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services"
	"github.com/ankur-anand/unisondb/pkg/replicator"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	reqIDKey = "request_id"
)

var (
	// batchWaitTime defines a timeout value after which even if batch size,
	// threshold is not met all the reads from replicator will be flushed onto the channel.
	batchWaitTime = time.Millisecond * 100

	// batchSize defines a size of batch.
	batchSize = 20

	grpcMaxMsgSize = 1 << 20
)

// GrpcStreamer implements gRPC-based WalStreamerService.
type GrpcStreamer struct {
	// namespace mapped engine
	storageEngines map[string]*dbkernel.Engine
	dynamicTimeout time.Duration
	v1.UnimplementedWalStreamerServiceServer
	errGrp   *errgroup.Group
	shutdown chan struct{}
}

// NewGrpcStreamer returns an initialized GrpcStreamer that implements  grpc-based WalStreamerService.
func NewGrpcStreamer(errGrp *errgroup.Group, storageEngines map[string]*dbkernel.Engine, dynamicTimeout time.Duration) *GrpcStreamer {
	return &GrpcStreamer{
		storageEngines: storageEngines,
		dynamicTimeout: dynamicTimeout,
		errGrp:         errGrp,
		shutdown:       make(chan struct{}),
	}
}

func (s *GrpcStreamer) Close() {
	close(s.shutdown)
}

// GetLatestOffset returns the latest wal offset of the wal for the provided namespace.
func (s *GrpcStreamer) GetLatestOffset(ctx context.Context, _ *v1.GetLatestOffsetRequest) (*v1.GetLatestOffsetResponse, error) {
	namespace, reqID, method := grpcutils.GetRequestInfo(ctx)

	if namespace == "" {
		return nil, services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := s.storageEngines[namespace]
	if !ok {
		return nil, services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}

	offset := engine.CurrentOffset()
	if offset == nil {
		return &v1.GetLatestOffsetResponse{}, nil
	}
	return &v1.GetLatestOffsetResponse{Offset: offset.Encode()}, nil
}

// StreamWalRecords stream the underlying WAL record on the connection stream.
func (s *GrpcStreamer) StreamWalRecords(request *v1.StreamWalRecordsRequest, g grpc.ServerStreamingServer[v1.StreamWalRecordsResponse]) error {
	namespace, reqID, method := grpcutils.GetRequestInfo(g.Context())

	if namespace == "" {
		return services.ToGRPCError(namespace, reqID, method, services.ErrMissingNamespaceInMetadata)
	}

	engine, ok := s.storageEngines[namespace]
	if !ok {
		return services.ToGRPCError(namespace, reqID, method, services.ErrNamespaceNotExists)
	}

	// it can contain terrible data
	meta, err := decodeMetadata(request.GetOffset())
	if err != nil {
		slog.Error("[unisondb.streamer.grpc]",
			slog.String("event_type", "metadata.decoding.failed"),
			slog.Any("error", err),
			slog.Any("server_offset", engine.CurrentOffset()),
			slog.Group("request",
				slog.String("namespace", namespace),
				slog.String("id", string(reqID)),
				slog.Any("offset", request.GetOffset()),
			),
		)
		return services.ToGRPCError(namespace, reqID, method, services.ErrInvalidMetadata)
	}
	// create a new replicator instance.
	slog.Debug("[unisondb.streamer.grpc] streaming WAL",
		"method", method,
		reqIDKey, reqID,
		"namespace", namespace,
		"offset", meta,
	)

	walReceiver := make(chan []*v1.WALRecord, 2)
	replicatorErr := make(chan error, 1)

	ctx, cancel := context.WithCancel(g.Context())
	defer cancel()

	rpInstance := replicator.NewReplicator(engine,
		batchSize,
		batchWaitTime, meta, "grpc")

	currentOffset := engine.CurrentOffset()
	if meta != nil && currentOffset == nil {
		return services.ToGRPCError(namespace, reqID, method, services.ErrInvalidMetadata)
	}

	// when server is closed, the goroutine would be closed upon
	// cancel of ctx.
	// walReceiver should be closed only when Replicate method returns.
	s.errGrp.Go(func() error {
		defer func() {
			close(walReceiver)
			close(replicatorErr)
		}()

		err := rpInstance.Replicate(ctx, walReceiver)
		select {
		case replicatorErr <- err:
		case <-ctx.Done():
			return nil
		}

		return nil
	})

	metricsActiveStreamTotal.WithLabelValues(namespace, string(method), "grpc").Inc()
	defer metricsActiveStreamTotal.WithLabelValues(namespace, string(method), "grpc").Dec()
	return s.streamWalRecords(ctx, g, walReceiver, replicatorErr)
}

// streamWalRecords streams namespaced Write-Ahead Log (WAL) records to the client in batches
// .
// While it might be tempting to stream WAL records indefinitely until the client cancels the RPC,
// this approach has significant drawbacks and is avoided here using dynamic timeout mechanism.
// (Note:  GOAWAY settings handle underlying connection issues.)
// Continuously streaming also has several problems:
//
//   - Resource Exhaustion: A malfunctioning client stream could monopolize the server's buffer,
//   - Unnecessary Processing: The server might fetch and stream WAL records that the client has already
//     received or is no longer interested in.
//
//nolint:gocognit
func (s *GrpcStreamer) streamWalRecords(ctx context.Context,
	g grpc.ServerStreamingServer[v1.StreamWalRecordsResponse],
	walReceiver chan []*v1.WALRecord,
	replicatorErr chan error) error {
	namespace, reqID, method := grpcutils.GetRequestInfo(g.Context())

	var (
		batch                  []*v1.WALRecord
		totalBatchSize         int
		lastReceivedRecordTime = time.Now()
	)

	flusher := func() error {
		if err := s.flushBatch(batch, g); err != nil {
			return err
		}
		batch = []*v1.WALRecord{}
		totalBatchSize = 0
		return nil
	}

	dynamicTimeoutTicker := time.NewTicker(s.dynamicTimeout)
	defer dynamicTimeoutTicker.Stop()

	for {
		select {
		case <-dynamicTimeoutTicker.C:
			if time.Since(lastReceivedRecordTime) > s.dynamicTimeout {
				return services.ToGRPCError(namespace, reqID, method, services.ErrStreamTimeout)
			}
		case <-s.shutdown:
			return status.Error(codes.Unavailable, internal.GracefulShutdownMsg)

		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, services.ErrStreamTimeout) {
				return services.ToGRPCError(namespace, reqID, method, services.ErrStreamTimeout)
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return ctx.Err()
		case walRecords := <-walReceiver:
			for _, walRecord := range walRecords {
				lastReceivedRecordTime = time.Now()
				totalBatchSize += len(walRecord.Record)
				batch = append(batch, walRecord)

				if totalBatchSize >= grpcMaxMsgSize {
					if err := flusher(); err != nil {
						return services.ToGRPCError(namespace, reqID, method, err)
					}
				}
			}
			// flush remaining
			if err := flusher(); err != nil {
				return services.ToGRPCError(namespace, reqID, method, err)
			}
		case err := <-replicatorErr:
			if errors.Is(err, dbkernel.ErrInvalidOffset) {
				slog.Error("[unisondb.streamer.grpc]",
					slog.String("event_type", "replicator.offset.invalid"),
					slog.Any("error", err),
					slog.Group("request",
						slog.String("namespace", namespace),
						slog.String("id", string(reqID)),
					),
				)

				return services.ToGRPCError(namespace, reqID, method, services.ErrInvalidMetadata)
			}
			return services.ToGRPCError(namespace, reqID, method, err)
		}
	}
}

func (s *GrpcStreamer) flushBatch(batch []*v1.WALRecord, g grpc.ServerStream) error {
	namespace, reqID, method := grpcutils.GetRequestInfo(g.Context())
	metricsStreamSendTotal.WithLabelValues(namespace, string(method), "grpc").Add(float64(len(batch)))
	if len(batch) == 0 {
		return nil
	}

	slog.Debug("[unisondb.streamer.grpc] Batch flushing", "size", len(batch))
	response := &v1.StreamWalRecordsResponse{Records: batch, ServerTimestamp: timestamppb.Now()}

	start := time.Now()
	defer func() {
		metricsStreamSendLatency.WithLabelValues(namespace, string(method), "grpc").Observe(time.Since(start).Seconds())
	}()

	if err := g.SendMsg(response); err != nil {
		slog.Error("[unisondb.streamer.grpc]",
			slog.String("event_type", "send.wal.records.failed"),
			slog.Any("error", err),
			slog.Int("records_count", len(batch)),
			slog.Group("request",
				slog.String("namespace", namespace),
				slog.String("id", string(reqID)),
			),
		)

		metricsStreamSendErrors.WithLabelValues(namespace, string(method), "grpc").Inc()
		return fmt.Errorf("failed to send WAL records: %w", err)
	}
	return nil
}

// decodeMetadata protects from panic and decodes.
func decodeMetadata(data []byte) (o *dbkernel.Offset, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decode ChunkPosition: Panic recovered %v", r)
		}
	}()
	if len(data) == 0 {
		return nil, nil
	}
	o = dbkernel.DecodeOffset(data)
	return o, err
}
