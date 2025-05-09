package wal

import (
	"io"
	"log"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/pkg/walfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/go-metrics"
	"github.com/pkg/errors"
)

var (
	walMetricsReadTotal       = append(packageKey, "wal", "read", "total")
	walMetricsReaderReadTotal = append(packageKey, "wal", "reader", "next", "read", "total")
	walMetricsAppendTotal     = append(packageKey, "wal", "append", "total")
	walMetricsReadLatency     = append(packageKey, "wal", "read", "durations", "seconds")
	walMetricsAppendLatency   = append(packageKey, "wal", "append", "durations", "seconds")
	walMetricsReadErrors      = append(packageKey, "wal", "read", "errors", "total")
	walMetricsAppendErrors    = append(packageKey, "wal", "append", "errors", "total")
	walMetricsReadBytes       = append(packageKey, "wal", "read", "bytes", "total")
	walMetricsAppendBytes     = append(packageKey, "wal", "append", "bytes", "total")
	walMetricsFSyncTotal      = append(packageKey, "wal", "fsync", "total")
	walMetricsFSyncErrors     = append(packageKey, "wal", "fsync", "errors", "total")
	walMetricsFSyncDurations  = append(packageKey, "wal", "fsync", "durations", "seconds")
)

// Offset is a type alias to underlying wal implementation.
type Offset = walfs.RecordPosition

func DecodeOffset(b []byte) *Offset {
	pos, _ := walfs.DecodeRecordPosition(b)
	return &pos
}

// WalIO provides a write and read to underlying file based wal store.
type WalIO struct {
	appendLog *walfs.WALog
	label     []metrics.Label
	metrics   *metrics.Metrics
}

func NewWalIO(dirname, namespace string, config *Config, m *metrics.Metrics) (*WalIO, error) {
	config.applyDefaults()
	wLog, err := walfs.NewWALog(dirname, ".seg", walfs.WithMaxSegmentSize(config.SegmentSize),
		walfs.WithMSyncEveryWrite(config.FSync), walfs.WithBytesPerSync(int64(config.BytesPerSync)))
	if err != nil {
		return nil, err
	}
	l := []metrics.Label{{Name: "namespace", Value: namespace}}

	return &WalIO{
			appendLog: wLog,
			label:     l,
			metrics:   m,
		},
		err
}

// Sync Flushes the wal using fsync.
func (w *WalIO) Sync() error {
	w.metrics.IncrCounterWithLabels(walMetricsFSyncTotal, 1, w.label)
	startTime := time.Now()
	defer func() {
		w.metrics.MeasureSinceWithLabels(walMetricsFSyncDurations, startTime, w.label)
	}()
	err := w.appendLog.Sync()
	if err != nil {
		w.metrics.IncrCounterWithLabels(walMetricsFSyncErrors, 1, w.label)
	}
	return err
}

func (w *WalIO) Close() error {
	err := w.Sync()
	if err != nil {
		slog.Error("[unisondb.wal] Fsync to log file failed]", "error", err)
		w.metrics.IncrCounterWithLabels(walMetricsFSyncErrors, 1, w.label)
	}
	return w.appendLog.Close()
}

func (w *WalIO) Read(pos *Offset) ([]byte, error) {
	w.metrics.IncrCounterWithLabels(walMetricsReadTotal, 1, w.label)
	startTime := time.Now()
	defer func() {
		w.metrics.MeasureSinceWithLabels(walMetricsReadLatency, startTime, w.label)
	}()
	value, err := w.appendLog.Read(*pos)
	if err != nil {
		w.metrics.IncrCounterWithLabels(walMetricsReadErrors, 1, w.label)
	}
	w.metrics.IncrCounterWithLabels(walMetricsReadBytes, float32(len(value)), w.label)
	return value, err
}

func (w *WalIO) Append(data []byte) (*Offset, error) {
	w.metrics.IncrCounterWithLabels(walMetricsAppendTotal, 1, w.label)
	startTime := time.Now()
	defer func() {
		w.metrics.MeasureSinceWithLabels(walMetricsAppendLatency, startTime, w.label)
	}()
	off, err := w.appendLog.Write(data)
	if errors.Is(err, walfs.ErrFsync) {
		log.Fatalf("[unisondb.wal] write to log file failed: %v", err)
	}
	if err != nil {
		w.metrics.IncrCounterWithLabels(walMetricsAppendErrors, 1, w.label)
	}
	w.metrics.IncrCounterWithLabels(walMetricsAppendBytes, float32(len(data)), w.label)
	return &off, err
}

type Reader struct {
	appendReader *walfs.Reader
	label        []metrics.Label
	metrics      *metrics.Metrics
	closed       atomic.Bool
}

// Next returns the next chunk data and its position in the WAL.
// If there is no data, io. EOF will be returned.
// The position can be used to read the data from the segment file.
func (r *Reader) Next() ([]byte, *Offset, error) {
	if r.closed.Load() {
		return nil, nil, io.EOF
	}
	r.metrics.IncrCounterWithLabels(walMetricsReaderReadTotal, 1, r.label)
	startTime := time.Now()
	defer func() {
		r.metrics.MeasureSinceWithLabels(walMetricsReadLatency, startTime, r.label)
	}()
	value, off, err := r.appendReader.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		r.metrics.IncrCounterWithLabels(walMetricsReadErrors, 1, r.label)
	}
	if errors.Is(err, io.EOF) {
		r.Close()
	}
	r.metrics.IncrCounterWithLabels(walMetricsReadBytes, float32(len(value)), r.label)
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
		label:        w.label,
		metrics:      w.metrics,
	}, nil
}

// NewReaderWithStart returns a new instance of WIOReader from the provided Offset.
func (w *WalIO) NewReaderWithStart(offset *Offset) (*Reader, error) {
	reader, err := w.appendLog.NewReaderWithStart(*offset)
	return &Reader{
		appendReader: reader,
		label:        w.label,
		metrics:      w.metrics,
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
