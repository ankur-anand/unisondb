package streamer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ankur-anand/unijord/partitionlog"
	"github.com/ankur-anand/unisondb/internal/services"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
)

const (
	blobStoreClientInitialBackoff = 500 * time.Millisecond
	blobStoreClientMaxBackoff     = 5 * time.Second
	blobStoreClientMaxRetries     = 5

	defaultRefreshInterval = 1 * time.Second
)

// BlobStoreStreamerClient implements the relayer.Streamer interface by reading
// committed WAL records from a partitionlog stream on object storage.
type BlobStoreStreamerClient struct {
	log       *partitionlog.Log
	namespace string
	wIO       WalIO

	mu sync.RWMutex
	// lsn of the record that was last received.
	lsn uint64

	refreshInterval time.Duration
}

// NewBlobStoreStreamerClient creates a new BlobStoreStreamerClient.
// startLSN is the LSN of the last record already applied; tailing resumes
// from the next record after this LSN — exactly like GrpcStreamerClient.
func NewBlobStoreStreamerClient(
	log *partitionlog.Log,
	namespace string,
	wIO WalIO,
	startLSN uint64,
	refreshInterval time.Duration,
) *BlobStoreStreamerClient {
	if refreshInterval <= 0 {
		refreshInterval = defaultRefreshInterval
	}

	return &BlobStoreStreamerClient{
		log:             log,
		namespace:       namespace,
		wIO:             wIO,
		lsn:             startLSN,
		refreshInterval: refreshInterval,
	}
}

// GetLatestLSN reads the latest committed LSN from partitionlog catalog head.
func (c *BlobStoreStreamerClient) GetLatestLSN(ctx context.Context) (uint64, error) {
	if c.log == nil {
		return 0, errors.New("blobstore client: nil partitionlog")
	}
	result, err := c.log.Reader().Partition(blobStorePartition).Read(ctx, partitionlog.ReadRequest{
		StartLSN:  math.MaxUint64,
		Limit:     1,
		Freshness: partitionlog.FreshnessLatest,
	})
	if err != nil {
		return 0, fmt.Errorf("blobstore client: read head: %w", err)
	}
	return previousLSN(result.Head.NextLSN), nil
}

// StreamWAL periodically checks the partitionlog head, reads newly committed
// WAL records, and applies them via the configured WalIO.
func (c *BlobStoreStreamerClient) StreamWAL(ctx context.Context) error {
	var retryCount int
	backoff := blobStoreClientInitialBackoff

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if retryCount > blobStoreClientMaxRetries {
			slog.Error("[unisondb.streamer.blobstore.client] Max retries reached, aborting WAL stream",
				"namespace", c.namespace, "retries", retryCount)
			clientWalStreamErrTotal.WithLabelValues(c.namespace, blobStoreLabel, "max_retries_reached").Inc()
			return fmt.Errorf("%w [%d]", services.ErrClientMaxRetriesExceeded, blobStoreClientMaxRetries)
		}

		err := c.streamWALRecords(ctx)
		if err == nil {
			retryCount = 0
			backoff = blobStoreClientInitialBackoff
			continue
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		var expired partitionlog.LSNExpiredError
		if errors.As(err, &expired) {
			clientWalStreamErrTotal.WithLabelValues(c.namespace, blobStoreLabel, "lsn_truncated").Inc()
			return fmt.Errorf("LSN %d truncated, resync required: %w", c.currentLSN(), err)
		}

		retryCount++
		slog.Warn("[unisondb.streamer.blobstore.client] tail failed, retrying",
			"namespace", c.namespace, "error", err,
			"retry_count", retryCount)
		clientWalStreamErrTotal.WithLabelValues(c.namespace, blobStoreLabel, "tail_error").Inc()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(blobStoreGetJitteredBackoff(&backoff)):
		}
	}
}

func (c *BlobStoreStreamerClient) streamWALRecords(ctx context.Context) error {
	if err := c.refreshAndApply(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		if err := c.refreshAndApply(ctx); err != nil {
			return err
		}
	}
}

func (c *BlobStoreStreamerClient) refreshAndApply(ctx context.Context) error {
	latestLSN, err := c.GetLatestLSN(ctx)
	if err != nil {
		return err
	}
	if latestLSN <= c.currentLSN() {
		return nil
	}
	return c.applyCommittedRange(ctx, latestLSN)
}

func (c *BlobStoreStreamerClient) applyCommittedRange(ctx context.Context, latestLSN uint64) error {
	partition := c.log.Reader().Partition(blobStorePartition)
	for c.currentLSN() < latestLSN {
		startLSN := c.currentLSN() + 1
		result, err := partition.Read(ctx, partitionlog.ReadRequest{
			StartLSN:  startLSN,
			Limit:     batchSize,
			Freshness: partitionlog.FreshnessCached,
		})
		if err != nil {
			return fmt.Errorf("blobstore client: read committed range: %w", err)
		}
		if len(result.Records) == 0 {
			return nil
		}

		batch := make([]*v1.WALRecord, 0, len(result.Records))
		var lastLSN uint64
		for _, record := range result.Records {
			batch = append(batch, &v1.WALRecord{Record: append([]byte(nil), record.Value...)})
			lastLSN = record.LSN
			clientWalRecvTotal.WithLabelValues(c.namespace, blobStoreLabel).Inc()
		}
		if err := c.wIO.WriteBatch(batch); err != nil {
			return fmt.Errorf("blobstore client: write batch: %w", err)
		}
		c.setLSN(lastLSN)
	}
	return nil
}

func (c *BlobStoreStreamerClient) currentLSN() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lsn
}

func (c *BlobStoreStreamerClient) setLSN(lsn uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lsn = lsn
}

func blobStoreGetJitteredBackoff(backoff *time.Duration) time.Duration {
	jitter := time.Duration(float64(*backoff) * (0.8 + 0.4*rand.Float64()))
	*backoff = min(*backoff*2, blobStoreClientMaxBackoff)
	return jitter
}
