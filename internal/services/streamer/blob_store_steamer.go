package streamer

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog"
	plazure "github.com/ankur-anand/unijord/partitionlog/azure"
	plgcs "github.com/ankur-anand/unijord/partitionlog/gcs"
	pls3 "github.com/ankur-anand/unijord/partitionlog/s3"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal"
	"github.com/ankur-anand/unisondb/pkg/replicator"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

const (
	blobStoreLabel     = "blobstore"
	blobStorePartition = uint32(0)

	defaultBlobStoreFlushInterval = time.Second
	defaultBlobStoreMaxRecords    = uint32(1_048_576)
)

// BlobStoreStreamer reads WAL records from storage engines and writes them to
// partitionlog segments on object storage. One namespace maps to one
// partitionlog stream and partition 0.
type BlobStoreStreamer struct {
	storageEngines map[string]*dbkernel.Engine
	namespaceLogs  map[string]*partitionlog.Log
	cfg            BlobStoreStreamerConfig
	errGrp         *errgroup.Group
	shutdown       chan struct{}
	mu             sync.Mutex
	namespaces     map[string]*blobStoreNamespaceState
	closed         bool
}

// BlobStoreStreamerConfig holds configuration for a BlobStoreStreamer.
type BlobStoreStreamerConfig struct {
	// FlushInterval cuts and publishes a non-empty segment after this duration.
	// Zero uses the default partitionlog batch age.
	FlushInterval time.Duration

	// BootstrapAfterLSN initializes an empty namespace at LSN+1. Existing object
	// storage state wins on restart; a configured bootstrap above existing state
	// is rejected to avoid skipping committed history.
	BootstrapAfterLSN map[string]uint64

	Batch        partitionlog.BatchPolicy
	Backpressure partitionlog.BackpressurePolicy
	Pipeline     partitionlog.WriterPipelineOptions
}

// DefaultBlobStoreStreamerConfig returns defaults for partitionlog-backed blob streaming.
func DefaultBlobStoreStreamerConfig() BlobStoreStreamerConfig {
	return BlobStoreStreamerConfig{
		FlushInterval: defaultBlobStoreFlushInterval,
		Batch: partitionlog.BatchPolicy{
			MaxRecords: defaultBlobStoreMaxRecords,
		},
	}
}

// NewBlobStoreStreamer returns an initialised BlobStoreStreamer.
// namespaceLogs must contain one partitionlog log per namespace.
func NewBlobStoreStreamer(
	ctx context.Context,
	errGrp *errgroup.Group,
	storageEngines map[string]*dbkernel.Engine,
	namespaceLogs map[string]*partitionlog.Log,
	cfg BlobStoreStreamerConfig,
) (*BlobStoreStreamer, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = defaultBlobStoreFlushInterval
	}
	if cfg.Batch.MaxDelay == 0 {
		cfg.Batch.MaxDelay = cfg.FlushInterval
	}
	if cfg.Batch.MaxRecords == 0 {
		cfg.Batch.MaxRecords = defaultBlobStoreMaxRecords
	}

	for namespace := range storageEngines {
		log, ok := namespaceLogs[namespace]
		if !ok || log == nil {
			return nil, fmt.Errorf("blobstore streamer: partitionlog for namespace %q not configured", namespace)
		}
	}

	return &BlobStoreStreamer{
		storageEngines: storageEngines,
		namespaceLogs:  namespaceLogs,
		cfg:            cfg,
		errGrp:         errGrp,
		shutdown:       make(chan struct{}),
		namespaces:     make(map[string]*blobStoreNamespaceState),
	}, nil
}

// StreamNamespace starts replicating WAL records for the given namespace into object storage.
// It blocks until the context is cancelled or the streamer is shut down.
func (s *BlobStoreStreamer) StreamNamespace(ctx context.Context, namespace string) error {
	engine, ok := s.storageEngines[namespace]
	if !ok {
		return fmt.Errorf("blobstore streamer: namespace %q not found", namespace)
	}
	nsState, err := s.namespaceState(ctx, namespace)
	if err != nil {
		return err
	}
	startLSN := nsState.startLSN

	walReceiver := make(chan []*v1.WALRecord, 2)
	replicatorErr := make(chan error, 1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.Debug("[unisondb.streamer.blobstore] streaming WAL",
		"namespace", namespace,
		"start_lsn", startLSN,
	)

	rpInstance := replicator.NewReplicator(engine, batchSize, batchWaitTime, startLSN, blobStoreLabel)

	if s.errGrp != nil {
		s.errGrp.Go(func() error { return replicateBlobStore(ctx, rpInstance, walReceiver, replicatorErr) })
	} else {
		go func() { _ = replicateBlobStore(ctx, rpInstance, walReceiver, replicatorErr) }()
	}

	metricsActiveStreamTotal.WithLabelValues(namespace, "StreamNamespace", blobStoreLabel).Inc()
	defer metricsActiveStreamTotal.WithLabelValues(namespace, "StreamNamespace", blobStoreLabel).Dec()

	return s.consumeAndWrite(ctx, namespace, nsState, walReceiver, replicatorErr)
}

func replicateBlobStore(ctx context.Context, rpInstance *replicator.Replicator, walReceiver chan []*v1.WALRecord, replicatorErr chan error) error {
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
}

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

		case walRecords, ok := <-walReceiver:
			if !ok {
				return nil
			}

			for _, walRecord := range walRecords {
				decoded := logrecord.GetRootAsLogRecord(walRecord.Record, 0)
				lsn := decoded.Lsn()

				if err := s.appendRecord(ctx, nsState, decoded, walRecord.Record); err != nil {
					slog.Error("[unisondb.streamer.blobstore]",
						slog.String("event_type", "append.failed"),
						slog.String("namespace", namespace),
						slog.Uint64("lsn", lsn),
						slog.Any("error", err),
					)
					replicator.ReleaseRecords(walRecords)
					return fmt.Errorf("blobstore streamer: append lsn %d: %w", lsn, err)
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

func (s *BlobStoreStreamer) appendRecord(ctx context.Context, nsState *blobStoreNamespaceState, decoded *logrecord.LogRecord, record []byte) error {
	lsn := decoded.Lsn()
	timestampMS := nsState.timestampMS(decoded.Hlc())
	result, err := nsState.writer.Append(ctx, partitionlog.Record{
		TimestampMS: timestampMS,
		Value:       record,
	})
	if err != nil {
		return s.failNamespace(nsState.namespace, nsState, err)
	}
	if result.LSN != lsn {
		return s.failNamespace(nsState.namespace, nsState,
			fmt.Errorf("partitionlog assigned lsn=%d for wal lsn=%d", result.LSN, lsn))
	}
	return nil
}

// Close shuts down the streamer and flushes active partitionlog writers.
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var errs []error
	for _, nsState := range namespaceStates {
		if err := nsState.close(ctx); err != nil {
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

	slog.Warn("[unisondb.streamer.blobstore]",
		slog.String("event_type", "namespace.failed"),
		slog.String("namespace", namespace),
		slog.Any("error", cause),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if closeErr := nsState.abort(ctx); closeErr != nil {
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
	log, ok := s.namespaceLogs[namespace]
	if !ok || log == nil {
		return nil, fmt.Errorf("blobstore streamer: partitionlog for namespace %q not configured", namespace)
	}

	bootstrapAfter := s.bootstrapAfterLSN(namespace)
	if bootstrapAfter == math.MaxUint64 {
		return nil, fmt.Errorf("blobstore streamer: bootstrap_after_lsn exhausted for %q", namespace)
	}
	bootstrapNext := bootstrapAfter + 1
	init, err := log.InitializePartition(ctx, partitionlog.InitializePartition{
		Partition: blobStorePartition,
		NextLSN:   bootstrapNext,
	})
	if err != nil {
		return nil, fmt.Errorf("blobstore streamer: initialize partition for %q: %w", namespace, err)
	}
	if !init.Created && init.Head.NextLSN < bootstrapNext {
		return nil, fmt.Errorf("blobstore streamer: namespace %q already starts before configured bootstrap_after_lsn: head_next_lsn=%d bootstrap_next_lsn=%d",
			namespace, init.Head.NextLSN, bootstrapNext)
	}

	writer, err := log.OpenWriter(ctx, partitionlog.WriterOptions{
		Partition:    blobStorePartition,
		WriterID:     writerIDForNamespace(namespace),
		Batch:        s.cfg.Batch,
		Backpressure: s.cfg.Backpressure,
		Pipeline:     s.cfg.Pipeline,
	})
	if err != nil {
		return nil, fmt.Errorf("blobstore streamer: open writer for %q: %w", namespace, err)
	}

	return &blobStoreNamespaceState{
		namespace:       namespace,
		log:             log,
		writer:          writer,
		startLSN:        previousLSN(init.Head.NextLSN),
		lastTimestampMS: lastTimestampMS(init.Head),
	}, nil
}

func (s *BlobStoreStreamer) bootstrapAfterLSN(namespace string) uint64 {
	if s.cfg.BootstrapAfterLSN == nil {
		return 0
	}
	return s.cfg.BootstrapAfterLSN[namespace]
}

type blobStoreNamespaceState struct {
	namespace string
	log       *partitionlog.Log
	writer    *partitionlog.Writer
	startLSN  uint64

	mu              sync.Mutex
	lastTimestampMS int64
	closeOnce       sync.Once
	closeErr        error
}

func (s *blobStoreNamespaceState) timestampMS(hlc uint64) int64 {
	now := time.Now().UTC().UnixMilli()
	timestamp := normalizeHLCTimestampMS(hlc, now)

	s.mu.Lock()
	defer s.mu.Unlock()
	if timestamp < s.lastTimestampMS {
		timestamp = s.lastTimestampMS
	}
	s.lastTimestampMS = timestamp
	return timestamp
}

func (s *blobStoreNamespaceState) close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		if s.writer != nil {
			_, s.closeErr = s.writer.Close(ctx)
		}
	})
	return s.closeErr
}

func (s *blobStoreNamespaceState) abort(ctx context.Context) error {
	s.closeOnce.Do(func() {
		if s.writer != nil {
			s.closeErr = s.writer.Abort(ctx)
		}
	})
	return s.closeErr
}

func normalizeHLCTimestampMS(hlc uint64, fallback int64) int64 {
	if hlc == 0 {
		return fallback
	}
	// Current Unix time in nanoseconds is ~1e18, while milliseconds is ~1e12.
	// Some WAL paths store HLC as nanoseconds; segment timestamps are millis.
	if hlc > 10_000_000_000_000_000 {
		return int64(hlc / uint64(time.Millisecond))
	}
	if hlc > uint64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(hlc)
}

func previousLSN(nextLSN uint64) uint64 {
	if nextLSN == 0 {
		return 0
	}
	return nextLSN - 1
}

func lastTimestampMS(head partitionlog.PartitionHead) int64 {
	if last, ok := head.Last(); ok {
		return last.MaxTimestampMS
	}
	return 0
}

func writerIDForNamespace(namespace string) [16]byte {
	sum := sha256.Sum256([]byte("unisondb/blobstore/" + namespace))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

func NamespaceBlobStorePrefix(basePrefix, namespace string) string {
	if namespace == "" {
		return basePrefix
	}
	return path.Join(basePrefix, namespace)
}

func OpenNamespacePartitionLog(ctx context.Context, bucketURL, basePrefix, namespace string) (*partitionlog.Log, error) {
	factory, err := newPartitionLogStoreFactory(ctx, bucketURL, basePrefix)
	if err != nil {
		return nil, err
	}
	return factory.openLog(namespace)
}

func OpenNamespacePartitionLogStore(ctx context.Context, bucketURL, basePrefix, namespace string) (partitionlog.Store, error) {
	factory, err := newPartitionLogStoreFactory(ctx, bucketURL, basePrefix)
	if err != nil {
		return nil, err
	}
	return factory.openStore(namespace)
}

func OpenNamespacePartitionLogs(ctx context.Context, bucketURL, basePrefix string, namespaces []string) (map[string]*partitionlog.Log, error) {
	factory, err := newPartitionLogStoreFactory(ctx, bucketURL, basePrefix)
	if err != nil {
		return nil, err
	}
	logs := make(map[string]*partitionlog.Log, len(namespaces))
	for _, namespace := range namespaces {
		log, err := factory.openLog(namespace)
		if err != nil {
			return nil, fmt.Errorf("open namespace partitionlog for %q: %w", namespace, err)
		}
		logs[namespace] = log
	}
	return logs, nil
}

type partitionLogStoreFactory struct {
	openStore func(namespace string) (partitionlog.Store, error)
}

func (f partitionLogStoreFactory) openLog(namespace string) (*partitionlog.Log, error) {
	store, err := f.openStore(namespace)
	if err != nil {
		return nil, err
	}
	log, err := partitionlog.Open(partitionlog.Options{
		Store: store,
		Reader: partitionlog.ReaderOptions{
			MaxRecordsPerBatch: batchSize,
			OpenSegmentReaders: 16,
		},
	})
	if err != nil {
		return nil, err
	}
	return log, nil
}

func newPartitionLogStoreFactory(ctx context.Context, bucketURL, basePrefix string) (partitionLogStoreFactory, error) {
	u, err := url.Parse(bucketURL)
	if err != nil {
		return partitionLogStoreFactory{}, fmt.Errorf("parse bucket url: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "s3":
		return newS3PartitionLogStoreFactory(ctx, u, basePrefix)
	case "gcs", "gs":
		return newGCSPartitionLogStoreFactory(ctx, u, basePrefix)
	case "azblob", "azure":
		return newAzurePartitionLogStoreFactory(ctx, u, basePrefix)
	default:
		return partitionLogStoreFactory{}, fmt.Errorf("unsupported partitionlog bucket scheme %q", u.Scheme)
	}
}

func newS3PartitionLogStoreFactory(ctx context.Context, u *url.URL, basePrefix string) (partitionLogStoreFactory, error) {
	if u.Host == "" {
		return partitionLogStoreFactory{}, fmt.Errorf("s3 bucket missing in url %q", u.String())
	}
	query := u.Query()
	region := query.Get("region")
	if region == "" {
		region = "us-east-1"
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}
	if mode, ok := checksumCalculation(query.Get("request_checksum_calculation")); ok {
		loadOpts = append(loadOpts, awsconfig.WithRequestChecksumCalculation(mode))
	}
	if mode, ok := checksumValidation(query.Get("response_checksum_validation")); ok {
		loadOpts = append(loadOpts, awsconfig.WithResponseChecksumValidation(mode))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return partitionLogStoreFactory{}, fmt.Errorf("load aws config: %w", err)
	}

	endpoint := query.Get("endpoint")
	usePathStyle := parseBoolDefault(query.Get("use_path_style"), false)
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = usePathStyle
	})

	return partitionLogStoreFactory{openStore: func(namespace string) (partitionlog.Store, error) {
		return pls3.New(pls3.Options{
			Client:   client,
			Bucket:   u.Host,
			Prefix:   strings.Trim(basePrefix, "/"),
			StreamID: namespace,
		})
	}}, nil
}

func newGCSPartitionLogStoreFactory(ctx context.Context, u *url.URL, basePrefix string) (partitionLogStoreFactory, error) {
	if u.Host == "" {
		return partitionLogStoreFactory{}, fmt.Errorf("gcs bucket missing in url %q", u.String())
	}
	query := u.Query()
	var opts []option.ClientOption
	if endpoint := query.Get("endpoint"); endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	if parseBoolDefault(query.Get("no_auth"), false) {
		opts = append(opts, option.WithoutAuthentication())
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return partitionLogStoreFactory{}, fmt.Errorf("create gcs client: %w", err)
	}
	return partitionLogStoreFactory{openStore: func(namespace string) (partitionlog.Store, error) {
		return plgcs.New(plgcs.Options{
			Client:   client,
			Bucket:   u.Host,
			Prefix:   strings.Trim(basePrefix, "/"),
			StreamID: namespace,
		})
	}}, nil
}

func newAzurePartitionLogStoreFactory(_ context.Context, u *url.URL, basePrefix string) (partitionLogStoreFactory, error) {
	containerName := strings.Trim(strings.TrimPrefix(path.Join(u.Host, u.Path), "/"), "/")
	if strings.Contains(containerName, "/") {
		return partitionLogStoreFactory{}, fmt.Errorf("azblob container url must identify exactly one container, got %q", containerName)
	}
	if containerName == "" {
		return partitionLogStoreFactory{}, fmt.Errorf("azblob container missing in url %q", u.String())
	}

	query := u.Query()
	connString := query.Get("connection_string")
	if connString == "" {
		connString = os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	}

	var (
		client *container.Client
		err    error
	)
	if connString != "" {
		client, err = container.NewClientFromConnectionString(connString, containerName, nil)
	} else {
		containerURL := query.Get("url")
		if containerURL == "" {
			endpoint := strings.TrimRight(query.Get("endpoint"), "/")
			if endpoint != "" {
				containerURL = endpoint + "/" + containerName
			}
		}
		if containerURL == "" {
			if account := query.Get("account"); account != "" {
				containerURL = "https://" + account + ".blob.core.windows.net/" + containerName
			}
		}
		if containerURL == "" {
			return partitionLogStoreFactory{}, errors.New("azblob bucket_url requires connection_string, AZURE_STORAGE_CONNECTION_STRING, url, endpoint, or account")
		}
		if sas := strings.TrimPrefix(query.Get("sas"), "?"); sas != "" && !strings.Contains(containerURL, "?") {
			containerURL += "?" + sas
		}
		switch strings.ToLower(query.Get("auth")) {
		case "default":
			cred, credErr := azidentity.NewDefaultAzureCredential(nil)
			if credErr != nil {
				return partitionLogStoreFactory{}, fmt.Errorf("create azure default credential: %w", credErr)
			}
			client, err = container.NewClient(containerURL, cred, nil)
		case "", "none", "sas":
			client, err = container.NewClientWithNoCredential(containerURL, nil)
		default:
			return partitionLogStoreFactory{}, fmt.Errorf("unsupported azblob auth mode %q", query.Get("auth"))
		}
	}
	if err != nil {
		return partitionLogStoreFactory{}, fmt.Errorf("create azure container client: %w", err)
	}

	return partitionLogStoreFactory{openStore: func(namespace string) (partitionlog.Store, error) {
		return plazure.New(plazure.Options{
			Container: client,
			Prefix:    strings.Trim(basePrefix, "/"),
			StreamID:  namespace,
		})
	}}, nil
}

func checksumCalculation(value string) (aws.RequestChecksumCalculation, bool) {
	switch strings.ToLower(value) {
	case "when_supported":
		return aws.RequestChecksumCalculationWhenSupported, true
	case "when_required":
		return aws.RequestChecksumCalculationWhenRequired, true
	default:
		return 0, false
	}
}

func checksumValidation(value string) (aws.ResponseChecksumValidation, bool) {
	switch strings.ToLower(value) {
	case "when_supported":
		return aws.ResponseChecksumValidationWhenSupported, true
	case "when_required":
		return aws.ResponseChecksumValidationWhenRequired, true
	default:
		return 0, false
	}
}

func parseBoolDefault(value string, fallback bool) bool {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
