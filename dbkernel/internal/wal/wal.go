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
	metricsReaderCreatedTotal  = "reader_created_total"
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
	if len(b) == 0 {
		return nil
	}
	pos, err := walfs.DecodeRecordPosition(b)
	if err != nil {
		return nil
	}
	return &pos
}

const (
	// We are sampling the read duration in the reader hot path.
	// we are avoiding random as one its more intensive than CPU masking in power2.
	// 16383 mask gives us sampling in the range of (1 in 16,384) 0.006%
	// So we only record few in hot reader path for each reader.
	samplingMask = 16383
)

// WalIO provides a write and read to underlying file based wal store.
type WalIO struct {
	appendLog        *walfs.WALog
	namespace        string
	taggedScope      umetrics.Scope
	entriesInSegment atomic.Int64
}

// NewWalIO initializes and returns a new instance of WalIO.
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

// RunWalCleanup starts background cleanup routines for old WAL segments.
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

// Close flushes WAL data to disk using fsync and then closes all underlying WAL segments.
func (w *WalIO) Close() error {
	err := w.Sync()
	if err != nil {
		slog.Error("[wal]", slog.String("message", "Failed to fsync WAL segment"),
			slog.Any("error", err))
		w.taggedScope.Counter(metricsFSyncErrors).Inc(1)
	}
	return w.appendLog.Close()
}

// Read retrieves the WAL record at the given position.
func (w *WalIO) Read(pos *Offset) ([]byte, error) {
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

// Append writes the provided data as a new record to the active WAL segment.
func (w *WalIO) Append(data []byte, logIndex uint64) (*Offset, error) {
	w.taggedScope.Counter(metricWalWriteTotal).Inc(1)
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		w.taggedScope.Histogram(metricWalWriteDuration, writeLatencyBuckets).RecordDuration(duration)
	}()
	w.taggedScope.Counter(metricWalBytesWrittenTotal).Inc(int64(len(data)))
	off, err := w.appendLog.Write(data, logIndex)
	if errors.Is(err, walfs.ErrFsync) {
		log.Fatalf("[wal] write to log file failed: %v", err)
	}
	if err != nil {
		w.taggedScope.Counter(metricWalWriteError).Inc(1)
	}
	if err == nil {
		w.entriesInSegment.Add(1)
	}
	return &off, err
}

// BatchAppend writes multiple records to the active WAL segment in a single batch operation.
// This is more efficient than calling Append multiple times as it reduces lock contention
// and can optimize disk I/O operations.
func (w *WalIO) BatchAppend(records [][]byte, logIndexes []uint64) ([]*Offset, error) {
	if len(records) == 0 {
		return nil, nil
	}

	w.taggedScope.Counter(metricWalWriteTotal).Inc(int64(len(records)))
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		w.taggedScope.Histogram(metricWalWriteDuration, writeLatencyBuckets).RecordDuration(duration)
	}()

	var totalBytes int64
	for _, data := range records {
		totalBytes += int64(len(data))
	}
	w.taggedScope.Counter(metricWalBytesWrittenTotal).Inc(totalBytes)

	offsets, err := w.appendLog.WriteBatch(records, logIndexes)
	if errors.Is(err, walfs.ErrFsync) {
		log.Fatalf("[wal] batch write to log file failed: %v", err)
	}
	if err != nil {
		w.taggedScope.Counter(metricWalWriteError).Inc(1)
		return nil, err
	}

	w.entriesInSegment.Add(int64(len(offsets)))

	result := make([]*Offset, len(offsets))
	for i := range offsets {
		result[i] = &offsets[i]
	}

	return result, nil
}

// BackupSegmentsAfter copies every sealed segment with ID greater than after into backupDir.
func (w *WalIO) BackupSegmentsAfter(after SegID, backupDir string) (map[SegID]string, error) {
	backups, err := w.appendLog.BackupSegmentsAfter(after, backupDir)
	if err != nil {
		return nil, err
	}

	result := make(map[SegID]string, len(backups))
	for id, path := range backups {
		result[id] = path
	}

	return result, nil
}

// ReaderOption defines a functional option for configuring a Reader instance.
type ReaderOption func(*Reader)

// WithActiveTail enables or disables active tail reading mode for the WAL reader.
func WithActiveTail(enabled bool) ReaderOption {
	return func(r *Reader) {
		r.withActiveTail = enabled
	}
}

// Reader provides a forward-only iterator over WAL records and is not concurrent safe.
type Reader struct {
	appendReader   *walfs.Reader
	closed         atomic.Bool
	namespace      string
	taggedScope    umetrics.Scope
	withActiveTail bool
	readCount      int
}

// Next returns the next chunk data and its position in the WAL.
// If there is no data, io. EOF will be returned.
// The position can be used to read the data from the segment file.
// The returned data is a memory-mapped slice — user must copy the data if retention of data is needed.
func (r *Reader) Next() ([]byte, Offset, error) {
	if r.closed.Load() {
		return nil, walfs.NilRecordPosition, io.EOF
	}

	// only sample every N reads
	// this doesn't give perfect latency measurement but is good enough to get overall picture
	// without adding too much overhead in hot path.
	// time.Now is only called when we are measuring.
	measure := r.readCount&samplingMask == 0

	var start time.Time
	if measure {
		start = time.Now()
	}

	data, pos, err := r.appendReader.Next()

	if err != nil {
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
	}

	r.readCount++
	if measure {
		dur := time.Since(start)
		r.taggedScope.Histogram(metricsWalReadDuration, readLatencyBuckets).RecordDuration(dur)
	}

	return data, pos, nil
}

// Close releases the underlying segment reader and marks the Reader as closed.
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

	w.taggedScope.Counter(metricsReaderCreatedTotal).Inc(1)
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

	w.taggedScope.Counter(metricsReaderCreatedTotal).Inc(1)
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
