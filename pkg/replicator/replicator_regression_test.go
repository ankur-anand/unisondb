package replicator

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/stretchr/testify/require"
)

func TestReplicator_ResumeAtTailWaitsForAppend(t *testing.T) {
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), "resume_at_tail", dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close(context.Background())) })
	require.NoError(t, engine.PutKV([]byte("key"), []byte("value")))

	startLSN := engine.OpsReceivedCount()
	rep := NewReplicator(engine, 2, time.Second, startLSN, "resume-at-tail")
	t.Cleanup(func() {
		if rep.reader != nil {
			rep.reader.Close()
		}
	})
	recordsChan := make(chan []*v1.WALRecord, 1)
	require.ErrorIs(t, rep.replicateFromReader(context.Background(), recordsChan), dbkernel.ErrNoNewData)
	require.Empty(t, recordsChan, "the resume record must not be sent again")

	waitResult := make(chan error, 1)
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		waitResult <- engine.WaitForAppendOrDone(rep.ctxDone, &rep.lastOffset)
	}()
	t.Cleanup(func() {
		close(rep.ctxDone)
		<-waitDone
	})

	select {
	case err := <-waitResult:
		t.Fatalf("caught-up wait returned before an append: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, engine.PutKV([]byte("next-key"), []byte("next-value")))
	select {
	case err := <-waitResult:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("append did not wake the replicator")
	}

	require.ErrorIs(t, rep.replicateFromReader(context.Background(), recordsChan), dbkernel.ErrNoNewData)
	require.Len(t, recordsChan, 1)
	batch := <-recordsChan
	defer ReleaseRecords(batch)
	require.Len(t, batch, 1)
	require.NotNil(t, batch[0])
	require.Equal(t, startLSN+1, logrecord.GetRootAsLogRecord(batch[0].Record, 0).Lsn())
}

func TestReplicator_CancellationNeverDeliversNilRecords(t *testing.T) {
	engine, err := dbkernel.NewStorageEngine(t.TempDir(), "cancel_delivery", dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close(context.Background())) })
	for i := 0; i < 20; i++ {
		require.NoError(t, engine.PutKV([]byte("key"), []byte("value")))
	}

	// Exercise both ready select cases: cancellation and delivery to a receiver.
	for attempt := 0; attempt < 256; attempt++ {
		rep := NewReplicator(engine, 1, time.Second, 0, "cancel-delivery")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		recordsChan := make(chan []*v1.WALRecord, 2)
		result := make(chan error, 1)
		go func() {
			result <- rep.Replicate(ctx, recordsChan)
			close(recordsChan)
		}()

		// Receiving the first batch ensures Replicate has entered its read loop.
		select {
		case batch := <-recordsChan:
			ReleaseRecords(batch)
		case <-ctx.Done():
			cancel()
			<-result
			t.Fatal("timed out waiting for the first batch")
		}
		cancel()

		nilRecords := 0
		for batch := range recordsChan {
			for _, record := range batch {
				if record == nil {
					nilRecords++
				}
			}
			ReleaseRecords(batch)
		}
		require.ErrorIs(t, <-result, context.Canceled)
		require.Zero(t, nilRecords, "released records were delivered on attempt %d", attempt)
	}
}
