package replicator

import (
	"context"
	"errors"
	"hash/crc32"
	"io"
	"time"

	"github.com/ankur-anand/unisondb/dbengine"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/replicator/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mKeyActiveReplicator = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kvalchemy",
		Subsystem: "replicator",
		Name:      "active",
	},
		[]string{"namespace", "replicator_engine"})

	mKeyReplicatorRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kvalchemy",
		Subsystem: "replicator",
		Name:      "records_total",
		Help:      "Total number of WAL records processed",
	}, []string{"namespace", "replicator_engine"})

	mKeyReplicatorBatchesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kvalchemy",
		Subsystem: "replicator",
		Name:      "batches_total",
		Help:      "Total number of WAL batches processed",
	}, []string{"namespace", "replicator_engine"})
)

// Replicator replicates from the engine and send batched wal records,
// as configured or if timeout expires.
type Replicator struct {
	engine           *dbengine.Engine
	batchSize        int
	batchDuration    time.Duration
	lastOffset       *dbengine.Offset
	replicatorEngine string
}

// NewReplicator returns an initialized Replicator that could be used for replicating
// the wal.
func NewReplicator(e *dbengine.Engine, batchSize int,
	batchDuration time.Duration,
	startOffset *dbengine.Offset, replicatorEngine string) *Replicator {
	return &Replicator{
		engine:           e,
		batchSize:        batchSize,
		batchDuration:    batchDuration,
		lastOffset:       startOffset,
		replicatorEngine: replicatorEngine,
	}
}

// Replicate reads wal record from the underlying engine,
// and sends the WalRecords when batchSize/maxBatchDuration is reached.
func (r *Replicator) Replicate(ctx context.Context, recordsChan chan<- []*v1.WALRecord) error {
	namespace := r.engine.Namespace()
	mKeyActiveReplicator.WithLabelValues(namespace, r.replicatorEngine).Inc()
	defer mKeyActiveReplicator.WithLabelValues(namespace, r.replicatorEngine).Dec()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := r.engine.WaitForAppend(ctx, r.batchDuration, r.lastOffset)

		if err != nil && !errors.Is(err, dbengine.ErrWaitTimeoutExceeded) {
			return err
		}

		err = r.replicateFromReader(ctx, recordsChan)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
		}
	}
}

// replicateFromReader reads the underlying wal until an err is encountered.
func (r *Replicator) replicateFromReader(ctx context.Context, recordsChan chan<- []*v1.WALRecord) error {
	timer := time.NewTimer(r.batchDuration)
	defer timer.Stop()

	namespace := r.engine.Namespace()

	var batch []*v1.WALRecord

	sendFunc := func() {
		if len(batch) > 0 {
			mKeyReplicatorRecordsTotal.WithLabelValues(namespace, r.replicatorEngine).Add(float64(len(batch)))
			mKeyReplicatorBatchesTotal.WithLabelValues(namespace, r.replicatorEngine).Add(1)
			select {
			case recordsChan <- batch:
				batch = []*v1.WALRecord{}
			case <-ctx.Done():
				return
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(r.batchDuration)
	}

	reader, err := r.getReader()
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			sendFunc()
		default:
		}

		value, pos, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				sendFunc()
				break
			}

			return err
		}

		walRecord := &v1.WALRecord{
			Offset:        pos.Encode(),
			Record:        value,
			Crc32Checksum: crc32.ChecksumIEEE(value),
		}

		batch = append(batch, walRecord)
		r.lastOffset = pos
		if len(batch) >= r.batchSize {
			sendFunc()
		}
	}

	return nil
}

func (r *Replicator) getReader() (*dbengine.Reader, error) {
	if r.lastOffset == nil {
		reader, err := r.engine.NewReader()
		return reader, err
	}

	reader, err := r.engine.NewReaderWithStart(r.lastOffset)
	if err != nil {
		return nil, err
	}

	// we consume the first record.
	_, _, err = reader.Next()
	if err != nil {
		return nil, err
	}

	return reader, err
}
