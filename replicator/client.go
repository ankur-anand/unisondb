package replicator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	v1 "github.com/ankur-anand/kvalchemy/proto/gen/go/kvalchemy/replicator/v1"
	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/ankur-anand/kvalchemy/storage/wrecord"
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

type StorageIO struct {
	engine    *storage.Engine
	namespace string
}

func NewStorageIO(engine *storage.Engine, namespace string) *StorageIO {
	return &StorageIO{}
}

func (io *StorageIO) Write(data *v1.WALRecord) {
	record := wrecord.GetRootAsWalRecord(data.CompressedData, 0)
	io.engine.Put(record.KeyBytes(), record.ValueBytes())
}

// WalIO provide.
type WalIO interface {
	Write(data *v1.WALRecord) error
}

type Client struct {
	gcc        *grpc.ClientConn
	namespaces string
	wIO        WalIO
	metadata   []byte
}

func (c *Client) StreamWAL(ctx context.Context) error {
	md := metadata.Pairs("x-namespace", c.namespaces)
	ctx = metadata.NewOutgoingContext(ctx, md)

	var retryCount int
	backoff := initialBackoff
	streamStartTime := time.Now()

	for {
		if retryCount > maxRetries {
			slog.Error("[REPLICATOR] Max retries reached, aborting WAL stream",
				"namespace", c.namespaces, "retries", retryCount)
			clientWalStreamErrTotal.WithLabelValues(c.namespaces, "max_retries_reached").Inc()
			return fmt.Errorf("max retries [%d] exceeded", maxRetries)
		}

		client, err := v1.NewWALReplicationServiceClient(c.gcc).StreamWAL(ctx, &v1.StreamWALRequest{
			Metadata: c.metadata,
		})

		if err != nil {
			if shouldRetry(err) {
				retryCount++
				slog.Warn("[REPLICATOR] StreamWAL failed, retrying",
					"namespace", c.namespaces, "error", err,
					"retry_count", retryCount)
				time.Sleep(getJitteredBackoff(&backoff))
				continue
			}
			return handleStreamError(c.namespaces, err)
		}

		if err := c.receiveWALRecords(client, streamStartTime); err != nil {
			if shouldRetry(err) {
				retryCount++
				slog.Warn("[REPLICATOR] StreamWAL failed, retrying",
					"namespace", c.namespaces, "error", err,
					"retry_count", retryCount)
				time.Sleep(getJitteredBackoff(&backoff))
				continue
			}
			return err
		}

		retryCount = 0
		backoff = initialBackoff
	}
}

func (c *Client) receiveWALRecords(client v1.WALReplicationService_StreamWALClient, startTime time.Time) error {
	for {
		res, err := client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				slog.Info("[REPLICATOR] stream closed",
					"namespace", c.namespaces,
					"stream_duration", humanizeDuration(time.Since(startTime)),
				)
				return nil
			}
			return err
		}

		clientWalRecvTotal.WithLabelValues(c.namespaces).Add(float64(len(res.WalRecords)))

		for _, record := range res.WalRecords {
			c.metadata = record.Metadata
			if err := c.wIO.Write(record); err != nil {
				return err
			}
		}
	}
}

func handleStreamError(namespace string, err error) error {
	sErr := status.Convert(err)
	clientWalStreamErrTotal.WithLabelValues(namespace, sErr.Code().String()).Inc()
	slog.Error("[REPLICATOR] Stream error", "namespace", namespace, "error", err)
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
