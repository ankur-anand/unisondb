package streamer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/prometheus/common/helpers/templates"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 5 * time.Second
	maxRetries     = 5
)

// WalIO provide.
type WalIO interface {
	Write(data *v1.WALRecord) error
	WriteBatch(records []*v1.WALRecord) error
}

type GrpcStreamerClient struct {
	gcc       *grpc.ClientConn
	namespace string
	wIO       WalIO
	// offset of the record that was last received.
	offset *dbkernel.Offset
}

func NewGrpcStreamerClient(gcc *grpc.ClientConn, namespace string, wIO WalIO, offset []byte) *GrpcStreamerClient {
	return &GrpcStreamerClient{
		gcc:       gcc,
		namespace: namespace,
		wIO:       wIO,
		offset:    dbkernel.DecodeOffset(offset),
	}
}

// GetLatestOffset returns the latest offset for the provided namespace that upstream has seen.
func (c *GrpcStreamerClient) GetLatestOffset(ctx context.Context) (*dbkernel.Offset, error) {
	md := metadata.Pairs("x-namespace", c.namespace)
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp, err := v1.NewWalStreamerServiceClient(c.gcc).GetLatestOffset(ctx, &v1.GetLatestOffsetRequest{})
	if err != nil {
		return nil, err
	}

	if len(resp.GetOffset()) == 0 {
		return nil, nil
	}
	return dbkernel.DecodeOffset(resp.GetOffset()), nil
}

// StreamWAL start the wal Streaming for the namespace from the upstream.
func (c *GrpcStreamerClient) StreamWAL(ctx context.Context) error {
	md := metadata.Pairs("x-namespace", c.namespace)
	ctx = metadata.NewOutgoingContext(ctx, md)

	var retryCount int
	backoff := initialBackoff
	streamStartTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if retryCount > maxRetries {
			slog.Error("[unisondb.streamer.grpc.client] Max retries reached, aborting WAL stream",
				"namespace", c.namespace, "retries", retryCount)
			clientWalStreamErrTotal.WithLabelValues(c.namespace, "grpc", "max_retries_reached").Inc()
			return fmt.Errorf("%w [%d]", services.ErrClientMaxRetriesExceeded, maxRetries)
		}

		client, err := v1.NewWalStreamerServiceClient(c.gcc).StreamWalRecords(ctx, &v1.StreamWalRecordsRequest{
			Offset: c.offset.Encode(),
		})

		if err != nil {
			if shouldRetry(err) {
				retryCount++
				slog.Warn("[unisondb.streamer.grpc.client] StreamWAL failed, retrying",
					"namespace", c.namespace, "error", err,
					"retry_count", retryCount)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(getJitteredBackoff(&backoff)):
				}
				continue
			}
			return handleStreamError(c.namespace, err)
		}

		if err := c.receiveWALRecords(client, streamStartTime); err != nil {
			if shouldRetry(err) {
				retryCount++
				slog.Warn("[unisondb.streamer.grpc.client] StreamWAL failed, retrying",
					"namespace", c.namespace, "error", err,
					"retry_count", retryCount)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(getJitteredBackoff(&backoff)):
				}
				continue
			}
			return err
		}

		retryCount = 0
		backoff = initialBackoff
	}
}

type batchJob struct {
	records []*v1.WALRecord
	offset  *dbkernel.Offset
}

func (c *GrpcStreamerClient) receiveWALRecords(client v1.WalStreamerService_StreamWalRecordsClient, startTime time.Time) error {
	batchChan := make(chan batchJob, 4)
	errChan := make(chan error, 1)

	go func() {
		for job := range batchChan {
			if err := c.wIO.WriteBatch(job.records); err != nil {
				errChan <- err
				return
			}
			c.offset = job.offset
		}
		errChan <- nil
	}()

	for {
		select {
		case err := <-errChan:
			return err
		default:
		}

		res, err := client.Recv()
		if err != nil {
			close(batchChan)
			writerErr := <-errChan

			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				if writerErr != nil {
					return writerErr
				}
				slog.Info("[unisondb.streamer.grpc.client] stream closed",
					"namespace", c.namespace,
					"stream_duration", humanizeDuration(time.Since(startTime)),
				)
				return nil
			}

			if writerErr != nil {
				return writerErr
			}
			return err
		}

		clientWalRecvTotal.WithLabelValues(c.namespace, "grpc").Add(float64(len(res.Records)))

		if len(res.Records) > 0 {
			lastRecord := res.Records[len(res.Records)-1]
			offset := &dbkernel.Offset{
				SegmentID: lastRecord.Offset.SegmentId,
				Offset:    int64(lastRecord.Offset.Offset),
			}

			select {
			case batchChan <- batchJob{records: res.Records, offset: offset}:
			case err := <-errChan:
				close(batchChan)
				return err
			}
		}
	}
}

func handleStreamError(namespace string, err error) error {
	sErr := status.Convert(err)
	clientWalStreamErrTotal.WithLabelValues(namespace, "grpc", sErr.Code().String()).Inc()
	slog.Error("[unisondb.streamer.grpc.client] Stream error", "namespace", namespace, "error", err)
	return err
}

func shouldRetry(err error) bool {
	sErr := status.Convert(err)
	return sErr.Code() == codes.Unavailable || sErr.Code() == codes.ResourceExhausted
}

func getJitteredBackoff(backoff *time.Duration) time.Duration {
	jitter := time.Duration(float64(*backoff) * (0.8 + 0.4*rand.Float64()))
	*backoff = min(*backoff*2, maxBackoff)
	return jitter
}

func humanizeDuration(d time.Duration) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		return d.String()
	}
	return s
}
