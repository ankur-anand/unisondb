package streamer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sync"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal"
	"github.com/ankur-anand/unisondb/pkg/replicator"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"golang.org/x/sync/errgroup"
)

const (
	blobStoreLabel           = "blobstore"
	blobStoreWriterClosedErr = "writer closed"

	blobStoreBackpressureInitialBackoff   = 10 * time.Millisecond
	blobStoreBackpressureMaxBackoff       = 250 * time.Millisecond
	defaultBlobStoreMaxImmutableMemtables = 4
)

// BlobStoreStreamer reads WAL records from storage engines via the Replicator
// and writes them to an isledb-backed blob store keyed by big-endian LSN.
// Downstream consumers use BlobStoreStreamerClient to refresh and consume.
type BlobStoreStreamer struct {
	storageEngines  map[string]*dbkernel.Engine
	namespaceStores map[string]*blobstore.Store
	writerOpts      isledb.WriterOptions
	errGrp          *errgroup.Group
	shutdown        chan struct{}
	mu              sync.Mutex
	namespaces      map[string]*blobStoreNamespaceState
	closed          bool
}

// BlobStoreStreamerConfig holds configuration for a BlobStoreStreamer.
type BlobStoreStreamerConfig struct {
	// FlushInterval controls how often the isledb writer background flusher runs.
	// When set, this overrides WriterOptions.FlushInterval.
	// Set it to 0 to use WriterOptions.FlushInterval as-is.
	FlushInterval time.Duration
	// WriterOptions allows customising the isledb writer.
	WriterOptions *isledb.WriterOptions
}

// DefaultBlobStoreStreamerConfig returns sensible defaults.
func DefaultBlobStoreStreamerConfig() BlobStoreStreamerConfig {
	wopts := isledb.DefaultWriterOptions()
	wopts.MaxImmutableMemtables = defaultBlobStoreMaxImmutableMemtables

	return BlobStoreStreamerConfig{
		FlushInterval: wopts.FlushInterval,
		WriterOptions: &wopts,
	}
}

// NewBlobStoreStreamer returns an initialised BlobStoreStreamer.
// namespaceStores must contain one blob store per namespace, already scoped
// to the target namespace prefix.
func NewBlobStoreStreamer(
	ctx context.Context,
	errGrp *errgroup.Group,
	storageEngines map[string]*dbkernel.Engine,
	namespaceStores map[string]*blobstore.Store,
	cfg BlobStoreStreamerConfig,
) (*BlobStoreStreamer, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	defaults := DefaultBlobStoreStreamerConfig()
	wopts := *defaults.WriterOptions
	if cfg.WriterOptions != nil {
		wopts = *cfg.WriterOptions
	}
	if cfg.FlushInterval != 0 {
		wopts.FlushInterval = cfg.FlushInterval
	}

	for namespace := range storageEngines {
		store, ok := namespaceStores[namespace]
		if !ok || store == nil {
			return nil, fmt.Errorf("blobstore streamer: store for namespace %q not configured", namespace)
		}
	}

	streamer := &BlobStoreStreamer{
		storageEngines:  storageEngines,
		namespaceStores: namespaceStores,
		writerOpts:      wopts,
		errGrp:          errGrp,
		shutdown:        make(chan struct{}),
		namespaces:      make(map[string]*blobStoreNamespaceState),
	}

	return streamer, nil
}

// StreamNamespace starts replicating WAL records for the given namespace into the blob store.
// It blocks until the context is cancelled or the streamer is shut down.
func (s *BlobStoreStreamer) StreamNamespace(ctx context.Context, namespace string, startLSN uint64) error {
	engine, ok := s.storageEngines[namespace]
	if !ok {
		return fmt.Errorf("blobstore streamer: namespace %q not found", namespace)
	}
	nsState, err := s.namespaceState(ctx, namespace)
	if err != nil {
		return err
	}

	walReceiver := make(chan []*v1.WALRecord, 2)
	replicatorErr := make(chan error, 1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.Debug("[unisondb.streamer.blobstore] streaming WAL",
		"namespace", namespace,
		"start_lsn", startLSN,
	)

	rpInstance := replicator.NewReplicator(engine,
		batchSize,
		batchWaitTime, startLSN, blobStoreLabel)

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

	metricsActiveStreamTotal.WithLabelValues(namespace, "StreamNamespace", blobStoreLabel).Inc()
	defer metricsActiveStreamTotal.WithLabelValues(namespace, "StreamNamespace", blobStoreLabel).Dec()

	return s.consumeAndWrite(ctx, namespace, nsState, walReceiver, replicatorErr)
}

// consumeAndWrite drains walReceiver batches and writes each record to isledb.
// Visibility is handled by the isledb writer background flusher.
//
//nolint:gocognit
func (s *BlobStoreStreamer) consumeAndWrite(
	ctx context.Context,
	namespace string,
	nsState *blobStoreNamespaceState,
	walReceiver chan []*v1.WALRecord,
	replicatorErr chan error,
) error {
	for {
		select {
		case <-s.shutdown:
			return fmt.Errorf("%s", internal.GracefulShutdownMsg)

		case <-ctx.Done():
			return ctx.Err()

		case <-nsState.flushErrSignal:
			metricsStreamSendErrors.WithLabelValues(namespace, "StreamNamespace", blobStoreLabel).Inc()
			return s.failNamespace(namespace, nsState,
				fmt.Errorf("blobstore streamer: background flush: %w", nsState.backgroundFlushError()))

		case walRecords, ok := <-walReceiver:
			if !ok {
				return nil
			}

			for _, walRecord := range walRecords {
				decoded := logrecord.GetRootAsLogRecord(walRecord.Record, 0)
				lsn := decoded.Lsn()

				if err := s.putRecordWithRetry(ctx, nsState, lsn, walRecord.Record); err != nil {
					replicator.ReleaseRecords(walRecords)
					return fmt.Errorf("blobstore streamer: put lsn %d: %w", lsn, err)
				}
			}

			metricsStreamSendTotal.WithLabelValues(namespace, "StreamNamespace", blobStoreLabel).Add(float64(len(walRecords)))
			replicator.ReleaseRecords(walRecords)

		case err := <-replicatorErr:
			if errors.Is(err, dbkernel.ErrInvalidOffset) {
				slog.Error("[unisondb.streamer.blobstore]",
					slog.String("event_type", "replicator.offset.invalid"),
					slog.Any("error", err),
					slog.String("namespace", namespace),
				)
				return fmt.Errorf("blobstore streamer: invalid offset: %w", err)
			}
			return err
		}
	}
}

func (s *BlobStoreStreamer) putRecordWithRetry(
	ctx context.Context,
	nsState *blobStoreNamespaceState,
	lsn uint64,
	record []byte,
) error {
	key := EncodeLSNKey(lsn)
	backoff := blobStoreBackpressureInitialBackoff

	for {
		err := nsState.writer.Put(key, record)
		if err == nil {
			return nil
		}
		if !errors.Is(err, isledb.ErrBackpressure) {
			if isTerminalNamespaceWriterError(err) {
				return s.failNamespace(nsState.namespace, nsState, err)
			}
			return err
		}

		timer := time.NewTimer(backoff)
		select {
		case <-s.shutdown:
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("%s", internal.GracefulShutdownMsg)
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-nsState.flushErrSignal:
			if !timer.Stop() {
				<-timer.C
			}
			return s.failNamespace(nsState.namespace, nsState,
				fmt.Errorf("background flush: %w", nsState.backgroundFlushError()))
		case <-timer.C:
		}

		backoff *= 2
		if backoff > blobStoreBackpressureMaxBackoff {
			backoff = blobStoreBackpressureMaxBackoff
		}
	}
}

// Close shuts down the streamer and releases isledb resources.
func (s *BlobStoreStreamer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.shutdown)
	namespaceStates := make([]*blobStoreNamespaceState, 0, len(s.namespaces))
	for _, nsState := range s.namespaces {
		namespaceStates = append(namespaceStates, nsState)
	}
	s.mu.Unlock()

	var errs []error
	for _, nsState := range namespaceStates {
		if err := nsState.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *BlobStoreStreamer) failNamespace(namespace string, nsState *blobStoreNamespaceState, cause error) error {
	s.mu.Lock()
	if current, ok := s.namespaces[namespace]; ok && current == nsState {
		delete(s.namespaces, namespace)
	}
	s.mu.Unlock()

	if errors.Is(cause, manifest.ErrFenced) {
		slog.Warn("[unisondb.streamer.blobstore]",
			slog.String("event_type", "writer.fenced"),
			slog.String("namespace", namespace),
			slog.Any("error", cause),
		)
	} else if isWriterClosedError(cause) {
		slog.Warn("[unisondb.streamer.blobstore]",
			slog.String("event_type", "writer.closed"),
			slog.String("namespace", namespace),
			slog.Any("error", cause),
		)
	}

	if closeErr := nsState.close(); closeErr != nil {
		return errors.Join(cause, closeErr)
	}
	return cause
}

func (s *BlobStoreStreamer) namespaceState(ctx context.Context, namespace string) (*blobStoreNamespaceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("blobstore streamer closed")
	}
	if nsState, ok := s.namespaces[namespace]; ok {
		return nsState, nil
	}

	nsState, err := s.openNamespaceState(ctx, namespace)
	if err != nil {
		return nil, err
	}
	s.namespaces[namespace] = nsState
	return nsState, nil
}

func (s *BlobStoreStreamer) openNamespaceState(ctx context.Context, namespace string) (*blobStoreNamespaceState, error) {
	store, ok := s.namespaceStores[namespace]
	if !ok || store == nil {
		return nil, fmt.Errorf("blobstore streamer: store for namespace %q not configured", namespace)
	}

	db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{
		CommittedLSNExtractor: isledb.BigEndianUint64LSNExtractor,
	})
	if err != nil {
		return nil, fmt.Errorf("blobstore streamer: open db for %q: %w", namespace, err)
	}

	nsState := &blobStoreNamespaceState{
		namespace:      namespace,
		store:          store,
		db:             db,
		flushErrSignal: make(chan struct{}),
	}

	wopts := s.writerOpts
	userOnFlushError := wopts.OnFlushError
	wopts.OnFlushError = func(err error) {
		nsState.recordFlushError(err)
		if userOnFlushError != nil {
			userOnFlushError(err)
		}
	}

	writer, err := db.OpenWriter(ctx, wopts)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("blobstore streamer: open writer for %q: %w", namespace, err)
	}
	nsState.writer = writer

	return nsState, nil
}

type blobStoreNamespaceState struct {
	namespace string
	store     *blobstore.Store
	db        *isledb.DB
	writer    *isledb.Writer

	flushErrMu     sync.RWMutex
	flushErr       error
	flushErrSignal chan struct{}
	flushErrOnce   sync.Once
	closeOnce      sync.Once
	closeErr       error
}

func (s *blobStoreNamespaceState) recordFlushError(err error) {
	if err == nil {
		return
	}

	s.flushErrOnce.Do(func() {
		s.flushErrMu.Lock()
		s.flushErr = err
		s.flushErrMu.Unlock()
		close(s.flushErrSignal)
	})
}

func (s *blobStoreNamespaceState) backgroundFlushError() error {
	s.flushErrMu.RLock()
	defer s.flushErrMu.RUnlock()

	if s.flushErr == nil {
		return errors.New("unknown flush error")
	}
	return s.flushErr
}

func (s *blobStoreNamespaceState) close() error {
	s.closeOnce.Do(func() {
		var errs []error
		if s.writer != nil {
			if err := s.writer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if s.db != nil {
			if err := s.db.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

func NamespaceBlobStorePrefix(basePrefix, namespace string) string {
	if namespace == "" {
		return basePrefix
	}
	return path.Join(basePrefix, namespace)
}

func OpenNamespaceBlobStore(ctx context.Context, bucketURL, basePrefix, namespace string) (*blobstore.Store, error) {
	return blobstore.Open(ctx, bucketURL, NamespaceBlobStorePrefix(basePrefix, namespace))
}

func OpenNamespaceBlobStores(ctx context.Context, bucketURL, basePrefix string, namespaces []string) (map[string]*blobstore.Store, error) {
	stores := make(map[string]*blobstore.Store, len(namespaces))
	for _, namespace := range namespaces {
		store, err := OpenNamespaceBlobStore(ctx, bucketURL, basePrefix, namespace)
		if err != nil {
			for _, opened := range stores {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open namespace blob store for %q: %w", namespace, err)
		}
		stores[namespace] = store
	}
	return stores, nil
}

func isTerminalNamespaceWriterError(err error) bool {
	return errors.Is(err, manifest.ErrFenced) || isWriterClosedError(err)
}

func isWriterClosedError(err error) bool {
	return err != nil && err.Error() == blobStoreWriterClosedErr
}

// EncodeLSNKey encodes a uint64 LSN as an 8-byte big-endian key
// so that keys sort lexicographically by LSN.
func EncodeLSNKey(lsn uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, lsn)
	return key
}

// DecodeLSNKey decodes an 8-byte big-endian key back to a uint64 LSN.
func DecodeLSNKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}
