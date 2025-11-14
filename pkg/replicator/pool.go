package replicator

import (
	"sync"

	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
)

var walRecordPool = sync.Pool{
	New: func() any {
		return &v1.WALRecord{}
	},
}

func acquireWalRecord() *v1.WALRecord {
	return walRecordPool.Get().(*v1.WALRecord)
}

func releaseWalRecord(record *v1.WALRecord) {
	if record == nil {
		return
	}
	record.Record = nil
	record.Offset = 0
	record.SegmentId = 0

	walRecordPool.Put(record)
}

// ReleaseRecords releases the WAL records back to the shared pool once callers
// are done processing them. Callers must ensure they no longer reference the
// records (e.g. after they have been serialized or applied).
func ReleaseRecords(records []*v1.WALRecord) {
	for i := range records {
		releaseWalRecord(records[i])
		records[i] = nil
	}
}
