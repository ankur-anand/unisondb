package replicator

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mKeyActiveReplicator = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "unisondb",
		Subsystem: "replicator",
		Name:      "active",
	},
		[]string{"namespace", "replicator_engine"})

	mKeyReplicatorRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "unisondb",
		Subsystem: "replicator",
		Name:      "records_total",
		Help:      "Total number of WAL records processed",
	}, []string{"namespace", "replicator_engine"})
)

// Replicator replicates from the engine and send batched wal records,
// as configured or if timeout expires.
type Replicator struct {
	engine           *dbkernel.Engine
	batchSize        int
	batchDuration    time.Duration
	lastOffset       dbkernel.Offset
	lastLSN          uint64 // LSN of the last record read (for LSN-based mode)
	useLSNMode       bool   // Use LSN-based replication instead of offset-based
	replicatorEngine string
	reader           *dbkernel.Reader
	ctxDone          chan struct{}
	namespace        string
}

// NewReplicator returns an initialized Replicator that could be used for replicating
// the wal.
func NewReplicator(e *dbkernel.Engine, batchSize int,
	batchDuration time.Duration,
	startOffset *dbkernel.Offset, replicatorEngine string) *Replicator {
	if startOffset == nil {
		startOffset = &dbkernel.Offset{}
	}
	return &Replicator{
		engine:           e,
		batchSize:        batchSize,
		batchDuration:    batchDuration,
		lastOffset:       *startOffset,
		replicatorEngine: replicatorEngine,
		ctxDone:          make(chan struct{}),
		namespace:        e.Namespace(),
	}
}

// NewReplicatorWithLSN returns an initialized Replicator that uses LSN-based replication.
func NewReplicatorWithLSN(e *dbkernel.Engine, batchSize int,
	batchDuration time.Duration,
	startLSN uint64, replicatorEngine string) *Replicator {
	return &Replicator{
		engine:           e,
		batchSize:        batchSize,
		batchDuration:    batchDuration,
		lastLSN:          startLSN,
		useLSNMode:       true,
		replicatorEngine: replicatorEngine,
		ctxDone:          make(chan struct{}),
		namespace:        e.Namespace(),
	}
}

// Replicate reads wal record from the underlying engine,
// and sends the WalRecords when batchSize/maxBatchDuration is reached.
func (r *Replicator) Replicate(ctx context.Context, recordsChan chan<- []*v1.WALRecord) error {
	namespace := r.engine.Namespace()
	mKeyActiveReplicator.WithLabelValues(namespace, r.replicatorEngine).Inc()
	defer mKeyActiveReplicator.WithLabelValues(namespace, r.replicatorEngine).Dec()

	// Start A Goroutine that will monitor the ctx check
	// If it happens it will close the reader first if any.
	once := sync.Once{}
	closeChannel := func() {
		once.Do(func() {
			close(r.ctxDone)
		})
	}
	go func() {
		<-ctx.Done()
		closeChannel()
	}()

	// just being safe.
	defer func() {
		closeChannel()
		if r.reader != nil {
			r.reader.Close()
		}
	}()

	for {
		if ctx.Err() != nil {
			closeChannel()
			if r.reader != nil {
				r.reader.Close()
			}
			return ctx.Err()
		}

		err := r.engine.WaitForAppendOrDone(r.ctxDone, &r.lastOffset)

		if err != nil && !errors.Is(err, dbkernel.ErrWaitTimeoutExceeded) {
			if r.reader != nil {
				r.reader.Close()
			}
			return err
		}

		err = r.replicateFromReader(ctx, recordsChan)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, dbkernel.ErrNoNewData) {
				continue
			}
			return err
		}
	}
}

// replicateFromReader reads the underlying wal until an err is encountered.
func (r *Replicator) replicateFromReader(ctx context.Context, recordsChan chan<- []*v1.WALRecord) error {
	batch := make([]*v1.WALRecord, 0, r.batchSize)
	sendFunc := func() {
		if len(batch) > 0 {
			mKeyReplicatorRecordsTotal.WithLabelValues(r.namespace, r.replicatorEngine).Add(float64(len(batch)))
			out := batch
			select {
			case recordsChan <- out:
				batch = make([]*v1.WALRecord, 0, r.batchSize)
			case <-r.ctxDone:
				ReleaseRecords(out)
				if r.reader != nil {
					r.reader.Close()
				}
				return
			}
		}
	}

	reader, err := r.getReader()
	if err != nil {
		return err
	}

	r.reader = reader
	for {
		value, pos, err := reader.Next()
		if err != nil {
			// irrespective of the error clear the batch
			sendFunc()
			if errors.Is(err, io.EOF) {
				reader.Close()
				r.reader = nil
				break
			}

			return err
		}

		walRecord := acquireWalRecord()
		walRecord.Record = value
		decoded := logrecord.GetRootAsLogRecord(value, 0)
		r.lastLSN = decoded.Lsn()

		batch = append(batch, walRecord)
		r.lastOffset = pos
		if len(batch) >= r.batchSize {
			sendFunc()
		}
	}

	return nil
}

func (r *Replicator) getReader() (*dbkernel.Reader, error) {
	if r.reader != nil {
		return r.reader, nil
	}

	if r.useLSNMode {
		reader, err := r.engine.NewReaderFromLSN(r.lastLSN, true)
		if err != nil {
			return nil, err
		}

		if r.lastLSN != 0 {
			_, _, err = reader.Next()
			if err != nil {
				return nil, err
			}
		}
		return reader, nil
	}

	// Offset-based
	if r.lastOffset.IsZero() {
		reader, err := r.engine.NewReaderWithTail(nil)
		return reader, err
	}

	reader, err := r.engine.NewReaderWithTail(&r.lastOffset)
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
