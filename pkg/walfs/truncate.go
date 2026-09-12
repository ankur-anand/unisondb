package walfs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"

	"github.com/edsrzf/mmap-go"
)

// A truncation intent is written atomically and synced before modifying any log
// files. It contains the desired header so even a torn target header is repairable.
// No new writes are allowed until the intent's removal has been synced.
type truncateIntent struct {
	Version   int         `json:"version"`
	KeepIndex uint64      `json:"keep_index"`
	Target    SegmentID   `json:"target"`
	Size      int64       `json:"size"`
	Header    []byte      `json:"header"`
	Remove    []SegmentID `json:"remove"`
}

type truncateEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	CRC     uint32          `json:"crc"`
}

// RecoveryError reports whether an interrupted truncation has made this handle
// unusable. Close and reopen the WAL to finish the durable intent in that case.
func (wl *WALog) RecoveryError() error {
	if err := wl.recoveryErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (wl *WALog) failTruncation(err error) error {
	failure := fmt.Errorf("%w: %w", ErrRecoveryRequired, err)
	wl.recoveryErr.Store(&failure)
	return failure
}

func (wl *WALog) truncateIntentPath() string {
	return filepath.Join(wl.dir, "truncate"+wl.ext+".intent")
}

// Truncate discards entries after logIndex, or resets the WAL when logIndex is 0.
// Validation failures leave storage unchanged. Once an intent is published, an
// I/O failure requires reopening the WAL so recovery can finish the operation.
// Callers must coordinate reader creation and advancement, and the lifetime of
// returned mmap slices, with truncation. Readers on segments being removed
// prevent truncation.
func (wl *WALog) Truncate(logIndex uint64) error {
	// The cleaner uses the same lock order. It must not delete a target or a
	// retained prefix while we prepare or execute a durable truncation.
	wl.deletionMu.Lock()
	defer wl.deletionMu.Unlock()
	wl.writeMu.Lock()
	defer wl.writeMu.Unlock()
	if err := wl.RecoveryError(); err != nil {
		return err
	}

	intent, entries, err := wl.prepareTruncation(logIndex)
	if err != nil {
		return err
	}
	published, err := wl.persistTruncation(intent)
	if err != nil {
		if published {
			return wl.failTruncation(err)
		}
		return err
	}
	if err := wl.applyTruncation(intent, entries); err != nil {
		return wl.failTruncation(err)
	}
	return nil
}

func (wl *WALog) applyTruncation(intent truncateIntent, entries []segmentIndexEntry) error {
	if intent.Target != 0 {
		seg := wl.segments[intent.Target]
		seg.lifecycleMu.Lock()
		seg.WaitForIndexFlush()
		seg.writeMu.Lock()
		err := seg.applyTruncate(entries)
		seg.writeMu.Unlock()
		seg.lifecycleMu.Unlock()
		if err != nil {
			return err
		}
		wl.currentSegment = seg
		// A formerly sealed target may have been queued for cleanup.
		delete(wl.pendingDeletion, intent.Target)
	}
	for _, id := range intent.Remove {
		seg := wl.segments[id]
		if seg == nil {
			continue
		}
		if err := seg.Remove(); err != nil {
			return err
		}
		delete(wl.segments, id)
		delete(wl.pendingDeletion, id)
	}
	if intent.KeepIndex == 0 {
		wl.currentSegment = nil
		wl.logIndex.Clear()
		wl.committedPos.Store(nil)
		seg, err := wl.openSegment(1)
		if err != nil {
			return err
		}
		wl.segments[1] = seg
		wl.currentSegment = seg
		if err := seg.Sync(); err != nil {
			return err
		}
	} else {
		_, last, ok := wl.logIndex.GetFirstLast()
		if ok && last > intent.KeepIndex {
			wl.logIndex.DeleteRange(intent.KeepIndex+1, last)
		}
	}
	wl.unSynced = 0
	wl.snapshotSegments()
	wl.recomputeBounds()
	if err := wl.clearTruncation(); err != nil {
		return err
	}
	return nil
}

func (wl *WALog) prepareTruncation(index uint64) (truncateIntent, []segmentIndexEntry, error) {
	intent := truncateIntent{Version: 1, KeepIndex: index}
	var target *Segment
	if index != 0 {
		// Empty segments have no first index and must never be selected as a target.
		for _, seg := range wl.segments {
			first := seg.FirstLogIndex()
			if first > 0 && first <= index && (target == nil || first > target.FirstLogIndex()) {
				target = seg
			}
		}
		if target == nil {
			return intent, nil, fmt.Errorf("truncate index %d not found in WAL", index)
		}
		intent.Target = target.ID()
	}
	for id, seg := range wl.segments {
		if index == 0 || id > intent.Target {
			// Include backups as well as readers; Remove otherwise waits for references.
			if seg.refCount.Load() != 0 {
				return intent, nil, fmt.Errorf("cannot delete segment %d: has active readers or backups", id)
			}
			intent.Remove = append(intent.Remove, id)
		}
	}
	// A reset creates segment 1 before retiring the intent. Include that file in
	// recovery even if prefix GC had already removed the original segment 1.
	if index == 0 && wl.segments[1] == nil {
		intent.Remove = append(intent.Remove, 1)
	}
	sort.Slice(intent.Remove, func(i, j int) bool { return intent.Remove[i] < intent.Remove[j] })
	if target == nil {
		return intent, nil, nil
	}
	if target.markedForDeletion.Load() {
		return intent, nil, fmt.Errorf("truncate target %d is already marked for deletion", target.ID())
	}
	target.lifecycleMu.Lock()
	defer target.lifecycleMu.Unlock()
	target.WaitForIndexFlush()
	target.writeMu.Lock()
	defer target.writeMu.Unlock()
	entries, err := target.prepareTruncateLocked(index)
	if err != nil {
		return intent, nil, err
	}
	// The intent may become durable before previously buffered appends. Persist
	// the retained prefix first so recovery can always honor the recorded cut.
	if err := target.Sync(); err != nil {
		return intent, nil, fmt.Errorf("sync retained prefix: %w", err)
	}
	last := entries[len(entries)-1]
	end := int64(last.Offset) + recordOverhead(int64(last.Length))
	intent.Size = target.mmapSize
	intent.Header = append([]byte(nil), target.mmapData[:segmentHeaderSize]...)
	setTruncateHeader(intent.Header, end, int64(len(entries)))
	return intent, entries, nil
}

// persistTruncation reports whether the intent was published, even if the
// following directory sync failed. Such a failure has an ambiguous durable state.
func (wl *WALog) persistTruncation(intent truncateIntent) (bool, error) {
	payload, err := json.Marshal(intent)
	if err != nil {
		return false, err
	}
	data, err := json.Marshal(truncateEnvelope{Payload: payload, CRC: crc32.Checksum(payload, crcTable)})
	if err != nil {
		return false, err
	}
	file, err := os.CreateTemp(wl.dir, "truncate-intent-*.tmp")
	if err != nil {
		return false, err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if _, err := file.Write(data); err != nil {
		return false, err
	}
	if err := file.Sync(); err != nil {
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(file.Name(), wl.truncateIntentPath()); err != nil {
		return false, err
	}
	return true, wl.dirSyncer.SyncDir(wl.dir)
}

func (wl *WALog) clearTruncation() error {
	if err := os.Remove(wl.truncateIntentPath()); err != nil {
		return err
	}
	return wl.dirSyncer.SyncDir(wl.dir)
}

func (wl *WALog) recoverTruncation() error {
	data, err := os.ReadFile(wl.truncateIntentPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var envelope truncateEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode truncation intent: %w", err)
	}
	if crc32.Checksum(envelope.Payload, crcTable) != envelope.CRC {
		return errors.New("truncation intent checksum mismatch")
	}
	var intent truncateIntent
	if err := json.Unmarshal(envelope.Payload, &intent); err != nil {
		return err
	}
	if err := intent.validate(); err != nil {
		return err
	}
	if intent.Target != 0 {
		if err := wl.recoverTruncateTarget(intent); err != nil {
			return err
		}
	}
	for _, id := range intent.Remove {
		for _, path := range []string{SegmentFileName(wl.dir, wl.ext, id), SegmentIndexFileName(wl.dir, wl.ext, id)} {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if err := wl.dirSyncer.SyncDir(wl.dir); err != nil {
		return err
	}
	return wl.clearTruncation()
}

func (in truncateIntent) validate() error {
	if in.Version != 1 {
		return fmt.Errorf("unsupported truncation intent version: %d", in.Version)
	}
	if in.KeepIndex == 0 {
		if in.Target != 0 || in.Size != 0 || len(in.Header) != 0 {
			return errors.New("invalid full truncation intent")
		}
	} else {
		if in.Target == 0 || in.Size < segmentHeaderSize || in.Size > maxSegmentSize || len(in.Header) != segmentHeaderSize {
			return errors.New("invalid truncation target")
		}
		meta, err := decodeSegmentHeader(in.Header)
		if err != nil {
			return err
		}
		if meta.WriteOffset <= segmentHeaderSize || meta.WriteOffset > in.Size || meta.EntryCount <= 0 || meta.FirstLogIndex == 0 || meta.FirstLogIndex > in.KeepIndex || in.KeepIndex-meta.FirstLogIndex != uint64(meta.EntryCount-1) || !IsActive(meta.Flags) || IsSealed(meta.Flags) {
			return errors.New("invalid truncation boundary")
		}
	}
	var previous SegmentID
	for _, id := range in.Remove {
		if id == 0 || id <= previous || (in.Target != 0 && id <= in.Target) {
			return errors.New("invalid truncation deletion set")
		}
		previous = id
	}
	return nil
}

func (wl *WALog) recoverTruncateTarget(intent truncateIntent) error {
	// Do not use OpenSegmentFile: a crash may have torn the old header, which the
	// durable intent replaces. Never create a missing target or resize its file.
	path := SegmentFileName(wl.dir, wl.ext, intent.Target)
	file, err := os.OpenFile(path, os.O_RDWR, fileModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != intent.Size {
		return fmt.Errorf("truncation target size changed: got %d, want %d", info.Size(), intent.Size)
	}
	if wl.markerValidator != nil {
		if err := wl.markerValidator(binary.LittleEndian.Uint32(intent.Header[52:56])); err != nil {
			return err
		}
	}
	data, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer data.Unmap()
	meta, err := decodeSegmentHeader(intent.Header)
	if err != nil {
		return err
	}
	seg := &Segment{fd: file, mmapData: data, mmapSize: info.Size(), path: path, id: intent.Target,
		firstLogIndex: meta.FirstLogIndex, indexPath: SegmentIndexFileName(wl.dir, wl.ext, intent.Target), dirSyncer: wl.dirSyncer}
	seg.writeOffset.Store(meta.WriteOffset)
	entries, err := seg.prepareTruncateLocked(intent.KeepIndex)
	if err != nil {
		return err
	}
	last := entries[len(entries)-1]
	if int64(last.Offset)+recordOverhead(int64(last.Length)) != meta.WriteOffset {
		return errors.New("truncation retained prefix does not match intent")
	}
	copy(data[:segmentHeaderSize], intent.Header)
	return seg.applyTruncate(entries)
}
