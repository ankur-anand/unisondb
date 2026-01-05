package replicator

import (
	"runtime"
	"testing"

	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
)

func TestReleaseRecordsResetsFields(t *testing.T) {
	record := acquireWalRecord()
	record.Record = []byte("payload")
	record.Crc32Checksum = 42

	ReleaseRecords([]*v1.WALRecord{record})

	pooled := acquireWalRecord()
	if pooled.Record != nil {
		t.Fatalf("expected pooled record payload to be nil, got %v", pooled.Record)
	}
	if pooled.Crc32Checksum != 0 {
		t.Fatalf("expected pooled checksum reset, got checksum=%d", pooled.Crc32Checksum)
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
			rec.Crc32Checksum = uint32(i*j + 1)
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
	if final.Crc32Checksum != 0 {
		t.Fatalf("pooled checksum not reset: checksum=%d", final.Crc32Checksum)
	}
	ReleaseRecords([]*v1.WALRecord{final})
}
