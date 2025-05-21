package streamer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	reqIDKey = "request_id"
)

var offsetPool = sync.Pool{
	New: func() any {
		return make([]byte, 12) // Fixed size for RecordPosition
	},
}

var walRecordPool = sync.Pool{
	New: func() any {
		return new(v1.WALRecord)
	},
}

var (
	// batchWaitTime defines a timeout value after which even if batch size,
	// threshold is not met all the reads from replicator will be flushed onto the channel.
	batchWaitTime = time.Millisecond * 100

	// batchSize defines a size of batch.
	batchSize = 20

	grpcMaxMsgSize = 1 << 20
)

var metricsWalReadLatency = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "unisondb",
		Subsystem: "streamer",
		Name:      "wal_batch_read_latency_seconds",
		Help:      "Latency between reads of WAL batches from the receiver",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
	},
	[]string{"namespace", "method"},
)

var metricsWalReceiverQueueSize = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "unisondb",
		Subsystem: "streamer",
		Name:      "wal_receiver_queue_size",
		Help:      "Number of WAL record batches currently buffered in the walReceiver channel",
	},
	[]string{"namespace"},
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

	//walReceiver := make(chan []*v1.WALRecord, 1000)
	//replicatorErr := make(chan error, 1)

	ctx, cancel := context.WithCancel(g.Context())
	defer cancel()

	//rpInstance := replicator.NewReplicator(engine,
	//	batchSize,
	//	batchWaitTime, meta, "grpc")

	currentOffset := engine.CurrentOffset()
	if meta != nil && currentOffset == nil {
		return services.ToGRPCError(namespace, reqID, method, services.ErrInvalidMetadata)
	}

	// when server is closed, the goroutine would be closed upon
	// cancel of ctx.
	// walReceiver should be closed only when Replicate method returns.
	//s.errGrp.Go(func() error {
	//	defer func() {
	//		close(walReceiver)
	//		close(replicatorErr)
	//	}()
	//
	//	err := rpInstance.Replicate(ctx, walReceiver)
	//	select {
	//	case replicatorErr <- err:
	//	case <-ctx.Done():
	//		return nil
	//	}
	//
	//	return nil
	//})

	metricsActiveStreamTotal.WithLabelValues(namespace, string(method), "grpc").Inc()
	defer metricsActiveStreamTotal.WithLabelValues(namespace, string(method), "grpc").Dec()
	return s.streamWalRecords(ctx, g, meta)
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
	meta *dbkernel.Offset) error {
	namespace, _, _ := grpcutils.GetRequestInfo(g.Context())

	var (
		batch []*v1.WALRecord
		//	//totalBatchSize         int
		//	//lastReceivedRecordTime = time.Now()
		//	//lastBatchReadTime      = time.Now()
	)

	engine := s.storageEngines[namespace]
	reader, err := getReader(nil, engine, meta)
	if err != nil {
		return err
	}
	ctxDoneSignal := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(ctxDoneSignal)

	}()
	defer func() {
		if reader != nil {
			reader.Close()
		}
	}()
	var lastSeen walfs.RecordPosition
	for {
		value, pos, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				reader.Close()
				reader = nil
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			err := engine.WaitForAppendOrDone(ctxDoneSignal, 2*time.Minute, &lastSeen)
			if err != nil && !errors.Is(err, dbkernel.ErrWaitTimeoutExceeded) {
				return err
			}
			reader, err = getReader(reader, engine, &lastSeen)
			if err != nil {
				slog.Error("[unisondb.streamer.grpc]", "err", err, "namespace", namespace)
				return err
			}
			continue
		}
		lastSeen = pos
		//
		buf := bytebufferpool.Get()         // *ByteBuffer
		buf.B = append(buf.B[:0], value...) // safely copy mmap-backed value

		offsetBuf := offsetPool.Get().([]byte)
		encodedOffset := walfs.EncodeRecordPositionTo(pos, offsetBuf)

		walRecord := walRecordPool.Get().(*v1.WALRecord)
		// use encodedOffset (e.g., assign to WALRecord.Offset)

		//offsetPool.Put(offsetBuf[:0]) // optional truncate for hygiene
		walRecord.Offset = encodedOffset
		walRecord.Record = buf.B
		// TODO: Get From the WAL Reader itself. Don't calculate here.
		//Crc32Checksum: crc32.Checksum(value, crcTable),

		batch = append(batch, walRecord)
		if len(batch) >= batchSize {
			err := s.flushBatch(batch, g)
			if err != nil {
				return err
			}
			for _, r := range batch {
				offsetPool.Put(r.Offset[:0])
				r.Offset = nil
				r.Record = nil
				walRecordPool.Put(r)
			}
			bytebufferpool.Put(buf)
			batch = nil
		}
	}
	//flusher := func() error {
	//	if err := s.flushBatch(batch, g); err != nil {
	//		return err
	//	}
	//	batch = []*v1.WALRecord{}
	//	totalBatchSize = 0
	//	return nil
	//}

	//dynamicTimeoutTicker := time.NewTicker(s.dynamicTimeout)
	//defer dynamicTimeoutTicker.Stop()
	//
	//ctxDoneSignal := make(chan struct{})
	//go func() {
	//	<-ctx.Done()
	//	close(ctxDoneSignal)
	//}()
	//
	//for {
	//	select {
	//	case <-dynamicTimeoutTicker.C:
	//		if time.Since(lastReceivedRecordTime) > s.dynamicTimeout {
	//			return services.ToGRPCError(namespace, reqID, method, services.ErrStreamTimeout)
	//		}
	//	case <-s.shutdown:
	//		return status.Error(codes.Unavailable, internal.GracefulShutdownMsg)
	//
	//	case <-ctxDoneSignal:
	//		err := ctx.Err()
	//		if errors.Is(err, services.ErrStreamTimeout) {
	//			return services.ToGRPCError(namespace, reqID, method, services.ErrStreamTimeout)
	//		}
	//		if errors.Is(err, context.Canceled) {
	//			return nil
	//		}
	//		return ctx.Err()
	//	case walRecords := <-walReceiver:
	//		metricsWalReceiverQueueSize.WithLabelValues(namespace).Set(float64(len(walReceiver)))
	//		readDelta := time.Since(lastBatchReadTime)
	//		lastBatchReadTime = time.Now()
	//		metricsWalReadLatency.WithLabelValues(namespace, string(method)).Observe(readDelta.Seconds())
	//		for _, walRecord := range walRecords {
	//			lastReceivedRecordTime = time.Now()
	//			totalBatchSize += len(walRecord.Record)
	//			//buf := make([]byte, len(walRecord.Record)-1)
	//			//copy(buf, walRecord.Record[1:])
	//			//walRecord.Record = buf
	//			batch = append(batch, walRecord)
	//
	//			if totalBatchSize >= grpcMaxMsgSize {
	//				if err := flusher(); err != nil {
	//					return services.ToGRPCError(namespace, reqID, method, err)
	//				}
	//			}
	//		}
	//		// flush remaining
	//		if err := flusher(); err != nil {
	//			return services.ToGRPCError(namespace, reqID, method, err)
	//		}
	//	case err := <-replicatorErr:
	//		if errors.Is(err, dbkernel.ErrInvalidOffset) {
	//			slog.Error("[unisondb.streamer.grpc]",
	//				slog.String("event_type", "replicator.offset.invalid"),
	//				slog.Any("error", err),
	//				slog.Group("request",
	//					slog.String("namespace", namespace),
	//					slog.String("id", string(reqID)),
	//				),
	//			)
	//
	//			return services.ToGRPCError(namespace, reqID, method, services.ErrInvalidMetadata)
	//		}
	//		return services.ToGRPCError(namespace, reqID, method, err)
	//	}
	//}
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

func getReader(existing *dbkernel.Reader, e *dbkernel.Engine, lastOffset *dbkernel.Offset) (*dbkernel.Reader, error) {
	if existing != nil {
		return existing, nil
	}

	if lastOffset == nil {
		reader, err := e.NewReaderWithTail(nil)
		return reader, err
	}

	reader, err := e.NewReaderWithTail(lastOffset)
	if err != nil {
		return nil, err
	}

	// we consume the first record.
	_, _, err = reader.Next()
	if err != nil {
		return nil, err
	}

	return reader, err
}
