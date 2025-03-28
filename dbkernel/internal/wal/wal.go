package wal

import (
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/ankur-anand/wal"
	"github.com/hashicorp/go-metrics"
	"github.com/pkg/errors"
)

var (
	walMetricsReadTotal      = append(packageKey, "wal", "read", "total")
	walMetricsAppendTotal    = append(packageKey, "wal", "append", "total")
	walMetricsReadLatency    = append(packageKey, "wal", "read", "durations", "seconds")
	walMetricsAppendLatency  = append(packageKey, "wal", "append", "durations", "seconds")
	walMetricsReadErrors     = append(packageKey, "wal", "read", "errors", "total")
	walMetricsAppendErrors   = append(packageKey, "wal", "append", "errors", "total")
	walMetricsReadBytes      = append(packageKey, "wal", "read", "bytes", "total")
	walMetricsAppendBytes    = append(packageKey, "wal", "append", "bytes", "total")
	walMetricsFSyncTotal     = append(packageKey, "wal", "fsync", "total")
	walMetricsFSyncErrors    = append(packageKey, "wal", "fsync", "errors", "total")
	walMetricsFSyncDurations = append(packageKey, "wal", "fsync", "durations", "seconds")
)

// Offset is a type alias to underlying wal implementation.
type Offset = wal.ChunkPosition

func DecodeOffset(b []byte) *Offset {
	return wal.DecodeChunkPosition(b)
}

// WalIO provides a write and read to underlying file based wal store.
type WalIO struct {
	appendLog *wal.WAL
	label     []metrics.Label
	metrics   *metrics.Metrics
}

func NewWalIO(dirname, namespace string, config *Config, m *metrics.Metrics) (*WalIO, error) {
	config.applyDefaults()
	w, err := wal.Open(newWALOptions(dirname, config))
	if err != nil {
		return nil, err
	}
	l := []metrics.Label{{Name: "namespace", Value: namespace}}

	return &WalIO{
			appendLog: w,
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
	_, err := w.appendLog.WriteAll()
	if err != nil {
		slog.Error("[kvalchemy.wal] write to log file failed]", "error", err)
		w.metrics.IncrCounterWithLabels(walMetricsAppendErrors, 1, w.label)
	}
	err = w.Sync()
	if err != nil {
		slog.Error("[kvalchemy.wal] Fsync to log file failed]", "error", err)
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
	value, err := w.appendLog.Read(pos)
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
	if err != nil {
		w.metrics.IncrCounterWithLabels(walMetricsAppendErrors, 1, w.label)
	}
	w.metrics.IncrCounterWithLabels(walMetricsAppendBytes, float32(len(data)), w.label)
	return off, err
}

type Reader struct {
	appendReader *wal.Reader
	label        []metrics.Label
	metrics      *metrics.Metrics
}

// Next returns the next chunk data and its position in the WAL.
// If there is no data, io. EOF will be returned.
// The position can be used to read the data from the segment file.
func (r *Reader) Next() ([]byte, *Offset, error) {
	startTime := time.Now()
	defer func() {
		r.metrics.MeasureSinceWithLabels(walMetricsReadLatency, startTime, r.label)
	}()
	value, off, err := r.appendReader.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		r.metrics.IncrCounterWithLabels(walMetricsReadErrors, 1, r.label)
	}
	r.metrics.IncrCounterWithLabels(walMetricsReadBytes, float32(len(value)), r.label)
	return value, off, err
}

func (r *Reader) CurrentSegmentID() uint32 {
	return r.appendReader.CurrentSegmentId()
}

func (r *Reader) CurrentOffset() *Offset {
	return r.appendReader.CurrentChunkPosition()
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
	reader, err := w.appendLog.NewReaderWithStart(offset)
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
