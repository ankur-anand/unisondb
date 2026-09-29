// Package walfs is UnisonDB's write-ahead log: memory-mapped segment files
// read without copying, and streamed to replicas while they are written.
//
// It is not a general-purpose WAL. It relies on how dbkernel uses it:
//
//   - Log indexes are contiguous. Each record's index is the previous one plus
//     one, and a record is located from its segment's first index and its
//     position in the segment. Writes that break the sequence fail with
//     ErrInvalidLogIndex.
//   - One writer, one process. A single WALog owns a directory; dbkernel's
//     pid.lock keeps other processes out.
//   - Reads are zero-copy. Returned slices point into the mapped file and stay
//     valid only while the reader still holds that segment; copy anything kept
//     longer.
//   - Records are readable before they are fsynced. Readers can see records a
//     crash will lose; the caller decides what to hand on, for example with
//     Commit and WithReaderCommitCheck.
//   - Retention only removes the oldest segments, when the caller's predicate
//     (dbkernel's B-tree checkpoint) allows it. A reader that needed a removed
//     segment gets ErrSegmentUnavailable and must start over from a snapshot.
//   - Truncate is for offline recovery tooling (udbctl wal truncate) on a WAL
//     nothing else has open, never for the running server.
package walfs
