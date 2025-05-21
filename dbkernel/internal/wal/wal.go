package wal

import (
	"context"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/pkg/umetrics"
	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/pkg/errors"
	"github.com/uber-go/tally/v4"
)

var segmentEntryBuckets = tally.MustMakeExponentialValueBuckets(500, 2, 10)

var (
	metricWalWriteTotal        = "write_total"
	metricWalWriteError        = "write_errors_total"
	metricWalWriteDuration     = "write_duration_seconds"
	metricsSegmentRotatedTotal = "segment_rotated_total"
	metricsWalReadTotal        = "read_total"
	metricsWalReadErrorTotal   = "read_error_total"
	metricsWalReadDuration     = "read_durations_second"
	metricsFSyncTotal          = "fsync_total"
	metricsFSyncErrors         = "fsync_errors_total"
	metricsFSyncDurations      = "fsync_durations_seconds"
	metricWalBytesWrittenTotal = "bytes_written_total"
	metricWalEntriesPerSegment = "entries_per_segment"
)

// Offset is a type alias to underlying wal implementation.
type Offset = walfs.RecordPosition

type SegID = walfs.SegmentID

func DecodeOffset(b []byte) *Offset {
	pos, _ := walfs.DecodeRecordPosition(b)
	return &pos
}

// WalIO provides a write and read to underlying file based wal store.
type WalIO struct {
	appendLog        *walfs.WALog
	namespace        string
	taggedScope      umetrics.Scope
	entriesInSegment atomic.Int64
}

func NewWalIO(dirname, namespace string, config *Config) (*WalIO, error) {
	config.applyDefaults()
	taggedScope := umetrics.AutoScope().Tagged(map[string]string{
		"namespace": namespace,
	})

	w := &WalIO{
		namespace:   namespace,
		taggedScope: taggedScope,
	}

	callbackOnRotate := func() {
		old := w.entriesInSegment.Swap(0)
		taggedScope.Counter(metricsSegmentRotatedTotal).Inc(1)
		taggedScope.Histogram(metricWalEntriesPerSegment, segmentEntryBuckets).RecordValue(float64(old))
	}

	wLog, err := walfs.NewWALog(dirname, ".seg", walfs.WithMaxSegmentSize(config.SegmentSize),
		walfs.WithMSyncEveryWrite(config.FSync), walfs.WithBytesPerSync(int64(config.BytesPerSync)),
		walfs.WithOnSegmentRotated(callbackOnRotate),
		walfs.WithAutoCleanupPolicy(config.MaxAge, config.MinSegment, config.MaxSegment, config.AutoCleanup))
	if err != nil {
		return nil, err
	}
	w.appendLog = wLog
	return w, err
}

func (w *WalIO) RunWalCleanup(ctx context.Context, interval time.Duration, predicate walfs.DeletionPredicate) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.appendLog.MarkSegmentsForDeletion()
			}
		}
	}()
	jitter := rand.Intn(10)
	dur := time.Duration(jitter)*time.Second + interval
	w.appendLog.StartPendingSegmentCleaner(ctx, dur, predicate)
}

// Sync Flushes the wal using fsync.
func (w *WalIO) Sync() error {
	w.taggedScope.Counter(metricsFSyncTotal).Inc(1)
	startTime := time.Now()
	defer func() {
		w.taggedScope.Histogram(metricsFSyncDurations, fsyncLatencyBuckets).RecordDuration(time.Since(startTime))
	}()
	err := w.appendLog.Sync()
	if err != nil {
		w.taggedScope.Counter(metricsFSyncErrors).Inc(1)
	}
	return err
}

func (w *WalIO) Close() error {
	err := w.Sync()
	if err != nil {
		slog.Error("[unisondb.wal] Fsync to log file failed]", "error", err)
		w.taggedScope.Counter(metricsFSyncErrors).Inc(1)
	}
	return w.appendLog.Close()
}

func (w *WalIO) Read(pos *Offset) ([]byte, error) {
	w.taggedScope.Counter(metricsWalReadTotal).Inc(1)
	startTime := time.Now()
	defer func() {
		w.taggedScope.Histogram(metricsWalReadDuration, readLatencyBuckets).RecordDuration(time.Since(startTime))
	}()
	value, err := w.appendLog.Read(*pos)
	if err != nil {
		w.taggedScope.Counter(metricsWalReadErrorTotal).Inc(1)
	}
	return value, err
}

func (w *WalIO) Append(data []byte) (*Offset, error) {
	w.taggedScope.Counter(metricWalWriteTotal).Inc(1)
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		w.taggedScope.Histogram(metricWalWriteDuration, writeLatencyBuckets).RecordDuration(duration)
	}()
	w.taggedScope.Counter(metricWalBytesWrittenTotal).Inc(int64(len(data)))
	off, err := w.appendLog.Write(data)
	if errors.Is(err, walfs.ErrFsync) {
		log.Fatalf("[unisondb.wal] write to log file failed: %v", err)
	}
	if err != nil {
		w.taggedScope.Counter(metricWalWriteError).Inc(1)
	}
	if err == nil {
		w.entriesInSegment.Add(1)
	}
	return &off, err
}

type ReaderOption func(*Reader)

func WithActiveTail(enabled bool) ReaderOption {
	return func(r *Reader) {
		r.withActiveTail = enabled
	}
}

type Reader struct {
	appendReader   *walfs.Reader
	closed         atomic.Bool
	namespace      string
	taggedScope    umetrics.Scope
	withActiveTail bool
}

// Next returns the next chunk data and its position in the WAL.
// If there is no data, io. EOF will be returned.
// The position can be used to read the data from the segment file.
func (r *Reader) Next() ([]byte, Offset, error) {
	if r.closed.Load() {
		return nil, walfs.NilRecordPosition, io.EOF
	}

	start := time.Now()
	data, pos, err := r.appendReader.Next()

	switch {
	case errors.Is(err, walfs.ErrNoNewData):
		if !r.withActiveTail {
			r.Close()
			return nil, walfs.NilRecordPosition, io.EOF
		}
		return nil, walfs.NilRecordPosition, err

	case errors.Is(err, io.EOF):
		r.Close()
		return nil, walfs.NilRecordPosition, err

	case err != nil:
		r.taggedScope.Counter(metricsWalReadErrorTotal).Inc(1)
		return nil, walfs.NilRecordPosition, err
	}

	// Successful read
	dur := time.Since(start)
	if rand.Float64() < 0.05 || dur > 10*time.Millisecond {
		r.taggedScope.Histogram(metricsWalReadDuration, readLatencyBuckets).RecordDuration(dur)
	}
	return data, pos, nil
}

func (r *Reader) Close() {
	if r.closed.CompareAndSwap(false, true) {
		r.appendReader.Close()
	}
}

// NewReader returns a new instance of WIOReader, allowing the caller to
// access WAL logs for replication, recovery, or log processing.
func (w *WalIO) NewReader(options ...ReaderOption) (*Reader, error) {
	reader := &Reader{
		appendReader: w.appendLog.NewReader(),
		namespace:    w.namespace,
		taggedScope:  w.taggedScope,
	}
	for _, opt := range options {
		opt(reader)
	}

	return reader, nil
}

// NewReaderWithStart returns a new instance of WIOReader from the provided Offset.
func (w *WalIO) NewReaderWithStart(offset *Offset, options ...ReaderOption) (*Reader, error) {
	underlyingReader, err := w.appendLog.NewReaderWithStart(*offset)
	if err != nil {
		return nil, err
	}
	reader := &Reader{
		appendReader: underlyingReader,
		namespace:    w.namespace,
		taggedScope:  w.taggedScope,
	}
	for _, opt := range options {
		opt(reader)
	}
	return reader, nil
}

// GetTransactionRecords returns all the WalRecord that is part of the particular Txn.
func (w *WalIO) GetTransactionRecords(startOffset *Offset) ([]*logrecord.LogRecord, error) {
	if startOffset == nil {
		return nil, nil
	}

	var records []*logrecord.LogRecord
	nextOffset := startOffset

	for {
		walEntry, err := w.Read(nextOffset)

		if err != nil {
			return nil, errors.Wrapf(ErrWalNextOffset, "failed to read WAL at offset %+v: %s", nextOffset, err)
		}

		record := logrecord.GetRootAsLogRecord(walEntry, 0)
		records = append(records, record)

		if record.PrevTxnWalIndexLength() == 0 {
			break
		}

		nextOffset = DecodeOffset(record.PrevTxnWalIndexBytes())
	}

	slices.Reverse(records)
	return records, nil
}
