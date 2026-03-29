package streamer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
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
// WAL records from an isledb-backed blob store using a refreshed Reader.
type BlobStoreStreamerClient struct {
	store     *blobstore.Store
	namespace string
	wIO       WalIO

	mu sync.RWMutex
	// lsn of the record that was last received.
	lsn uint64
	// reader is set while StreamWAL is actively running so monitoring can reuse
	// the live reader instead of opening a new one.
	reader *isledb.Reader

	refreshInterval time.Duration
	CacheDir        string
}

// NewBlobStoreStreamerClient creates a new BlobStoreStreamerClient.
// The provided store must already be scoped to the target namespace prefix.
// startLSN is the LSN of the last record already applied; tailing resumes
// from the next record after this LSN — exactly like GrpcStreamerClient.
// refreshInterval controls how often the reader refreshes its manifest and
// checks for newly committed records. Zero or negative values use the default.
func NewBlobStoreStreamerClient(
	store *blobstore.Store,
	namespace string,
	wIO WalIO,
	startLSN uint64,
	refreshInterval time.Duration,
) *BlobStoreStreamerClient {
	if refreshInterval <= 0 {
		refreshInterval = defaultRefreshInterval
	}

	return &BlobStoreStreamerClient{
		store:           store,
		namespace:       namespace,
		wIO:             wIO,
		lsn:             startLSN,
		refreshInterval: refreshInterval,
	}
}

// GetLatestLSN reads the latest committed LSN from isledb CURRENT metadata.
func (c *BlobStoreStreamerClient) GetLatestLSN(ctx context.Context) (uint64, error) {
	if reader := c.activeReader(); reader != nil {
		lsn, found, err := reader.MaxCommittedLSN(ctx)
		if err != nil {
			return 0, fmt.Errorf("blobstore client: read max committed lsn from active reader: %w", err)
		}
		if !found {
			return 0, nil
		}
		return lsn, nil
	}

	return c.getLatestLSNFromReader(ctx)
}

func (c *BlobStoreStreamerClient) getLatestLSNFromReader(ctx context.Context) (uint64, error) {
	readerOpts := isledb.ReaderOpenOptions{}
	if c.CacheDir != "" {
		readerOpts.CacheDir = c.CacheDir
	}

	reader, err := isledb.OpenReader(ctx, c.store, readerOpts)
	if err != nil {
		return 0, fmt.Errorf("blobstore client: open reader: %w", err)
	}
	defer reader.Close()

	lsn, found, err := reader.MaxCommittedLSN(ctx)
	if err != nil {
		return 0, fmt.Errorf("blobstore client: read max committed lsn: %w", err)
	}
	if !found {
		return 0, nil
	}
	return lsn, nil
}

// StreamWAL periodically refreshes the blob store reader, scans newly
// committed WAL records, and applies them via the configured WalIO.
// It implements the relayer.Streamer interface.
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
	readerOpts := isledb.ReaderOpenOptions{}
	if c.CacheDir != "" {
		readerOpts.CacheDir = c.CacheDir
	}

	reader, err := isledb.OpenReader(ctx, c.store, readerOpts)
	if err != nil {
		return fmt.Errorf("blobstore client: open reader: %w", err)
	}
	defer func() {
		c.clearActiveReader(reader)
		_ = reader.Close()
	}()
	c.setActiveReader(reader)

	if err := c.refreshAndApply(ctx, reader); err != nil {
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

		if err := c.refreshAndApply(ctx, reader); err != nil {
			return err
		}
	}
}

func (c *BlobStoreStreamerClient) refreshAndApply(ctx context.Context, reader *isledb.Reader) error {
	if err := reader.Refresh(ctx); err != nil {
		return fmt.Errorf("blobstore client: refresh reader: %w", err)
	}

	latestLSN, found, err := reader.MaxCommittedLSN(ctx)
	if err != nil {
		return fmt.Errorf("blobstore client: read max committed lsn: %w", err)
	}
	if !found || latestLSN <= c.currentLSN() {
		return nil
	}

	return c.applyCommittedRange(ctx, reader, latestLSN)
}

func (c *BlobStoreStreamerClient) applyCommittedRange(ctx context.Context, reader *isledb.Reader, latestLSN uint64) error {
	startLSN := c.currentLSN() + 1
	iterOpts := isledb.IteratorOptions{
		MaxKey: EncodeLSNKey(latestLSN),
	}
	if startLSN > 0 {
		iterOpts.MinKey = EncodeLSNKey(startLSN)
	}

	iter, err := reader.NewIterator(ctx, iterOpts)
	if err != nil {
		return fmt.Errorf("blobstore client: open iterator: %w", err)
	}
	defer iter.Close()

	var batch []*v1.WALRecord
	var lastLSN uint64

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := c.wIO.WriteBatch(batch); err != nil {
			return fmt.Errorf("blobstore client: write batch: %w", err)
		}
		c.setLSN(lastLSN)
		batch = nil
		return nil
	}

	for iter.Next() {
		key := iter.Key()
		if len(key) != 8 {
			slog.Warn("[unisondb.streamer.blobstore.client] skipping malformed key",
				"namespace", c.namespace,
				"key_len", len(key))
			continue
		}

		lastLSN = DecodeLSNKey(key)
		batch = append(batch, &v1.WALRecord{Record: iter.Value()})
		clientWalRecvTotal.WithLabelValues(c.namespace, blobStoreLabel).Inc()

		if len(batch) >= batchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("blobstore client: iterate wal range: %w", err)
	}
	if err := flushBatch(); err != nil {
		return err
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

func (c *BlobStoreStreamerClient) activeReader() *isledb.Reader {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.reader
}

func (c *BlobStoreStreamerClient) setActiveReader(reader *isledb.Reader) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reader = reader
}

func (c *BlobStoreStreamerClient) clearActiveReader(reader *isledb.Reader) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reader == reader {
		c.reader = nil
	}
}

func blobStoreGetJitteredBackoff(backoff *time.Duration) time.Duration {
	jitter := time.Duration(float64(*backoff) * (0.8 + 0.4*rand.Float64()))
	*backoff = min(*backoff*2, blobStoreClientMaxBackoff)
	return jitter
}
