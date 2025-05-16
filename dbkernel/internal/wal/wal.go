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
)

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
	appendLog   *walfs.WALog
	namespace   string
	taggedScope umetrics.Scope
}

func NewWalIO(dirname, namespace string, config *Config) (*WalIO, error) {
	config.applyDefaults()
	taggedScope := umetrics.AutoScope().Tagged(map[string]string{
		"namespace": namespace,
	})
	callbackOnRotate := func() {
		taggedScope.Counter(metricsSegmentRotatedTotal).Inc(1)
	}
	wLog, err := walfs.NewWALog(dirname, ".seg", walfs.WithMaxSegmentSize(config.SegmentSize),
		walfs.WithMSyncEveryWrite(config.FSync), walfs.WithBytesPerSync(int64(config.BytesPerSync)),
		walfs.WithOnSegmentRotated(callbackOnRotate),
		walfs.WithAutoCleanupPolicy(config.MaxAge, config.MinSegment, config.MaxSegment, config.AutoCleanup))
	if err != nil {
		return nil, err
	}

	return &WalIO{
			appendLog:   wLog,
			namespace:   namespace,
			taggedScope: taggedScope,
		},
		err
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

	off, err := w.appendLog.Write(data)
	if errors.Is(err, walfs.ErrFsync) {
		log.Fatalf("[unisondb.wal] write to log file failed: %v", err)
	}
	if err != nil {
		w.taggedScope.Counter(metricWalWriteError).Inc(1)
	}
	return &off, err
}

type Reader struct {
	appendReader *walfs.Reader
	closed       atomic.Bool
	namespace    string
	taggedScope  umetrics.Scope
}

// Next returns the next chunk data and its position in the WAL.
// If there is no data, io. EOF will be returned.
// The position can be used to read the data from the segment file.
func (r *Reader) Next() ([]byte, *Offset, error) {
	if r.closed.Load() {
		return nil, nil, io.EOF
	}
	startTime := time.Now()
	value, off, err := r.appendReader.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		r.taggedScope.Counter(metricsWalReadTotal).Inc(1)
		r.taggedScope.Counter(metricsWalReadErrorTotal).Inc(1)
	}
	if errors.Is(err, io.EOF) {
		r.Close()
	}
	if err == nil {
		duration := time.Since(startTime)
		// we are sampling only 5% of fast-path reads, but always capture slow reads
		if rand.Float64() < 0.05 || duration > 10*time.Millisecond {
			r.taggedScope.Histogram(metricsWalReadDuration, readLatencyBuckets).RecordDuration(duration)
		}
		r.taggedScope.Counter(metricsWalReadTotal).Inc(1)
	}
	return value, off, err
}

func (r *Reader) Close() {
	if r.closed.CompareAndSwap(false, true) {
		r.appendReader.Close()
	}
}

// NewReader returns a new instance of WIOReader, allowing the caller to
// access WAL logs for replication, recovery, or log processing.
func (w *WalIO) NewReader() (*Reader, error) {
	return &Reader{
		appendReader: w.appendLog.NewReader(),
		namespace:    w.namespace,
		taggedScope:  w.taggedScope,
	}, nil
}

// NewReaderWithStart returns a new instance of WIOReader from the provided Offset.
func (w *WalIO) NewReaderWithStart(offset *Offset) (*Reader, error) {
	reader, err := w.appendLog.NewReaderWithStart(*offset)
	return &Reader{
		appendReader: reader,
		namespace:    w.namespace,
		taggedScope:  w.taggedScope,
	}, err
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
