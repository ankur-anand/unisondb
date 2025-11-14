package replicator

import (
	"runtime"
	"testing"

	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
)

func TestReleaseRecordsResetsFields(t *testing.T) {
	record := acquireWalRecord()
	record.Record = []byte("payload")
	record.Offset = 42
	record.SegmentId = 9

	ReleaseRecords([]*v1.WALRecord{record})

	pooled := acquireWalRecord()
	if pooled.Record != nil {
		t.Fatalf("expected pooled record payload to be nil, got %v", pooled.Record)
	}
	if pooled.Offset != 0 || pooled.SegmentId != 0 {
		t.Fatalf("expected pooled offset reset, got offset=%d segment=%d", pooled.Offset, pooled.SegmentId)
	}

	ReleaseRecords([]*v1.WALRecord{pooled})
}

func TestReleaseRecordsAllowsGC(t *testing.T) {
	const iterations = 10000
	records := make([][]*v1.WALRecord, iterations)
	for i := 0; i < iterations; i++ {
		batch := make([]*v1.WALRecord, 0, 4)
		for j := 0; j < 4; j++ {
			rec := acquireWalRecord()
			rec.Record = make([]byte, 256)
			rec.Offset = uint64(i*j + 1)
			rec.SegmentId = uint32(i + j)
			batch = append(batch, rec)
		}
		records[i] = batch
	}

	for _, batch := range records {
		ReleaseRecords(batch)
	}

	runtime.GC()

	final := acquireWalRecord()
	if final.Record != nil {
		t.Fatalf("pooled record retains payload reference, potential leak")
	}
	if final.Offset != 0 || final.SegmentId != 0 {
		t.Fatalf("pooled offset not reset: offset=%d segment=%d", final.Offset, final.SegmentId)
	}
	ReleaseRecords([]*v1.WALRecord{final})
}
