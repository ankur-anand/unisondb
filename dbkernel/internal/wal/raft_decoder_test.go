package wal

import (
	"bytes"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/pkg/raftwalfs"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/hashicorp/raft"
)

func TestRaftWALDecoder_Decode(t *testing.T) {
	codec := raftwalfs.BinaryCodecV1{}
	decoder := NewRaftWALDecoder(codec)

	t.Run("decodes LogCommand and returns Data", func(t *testing.T) {
		payload := []byte("test payload data")
		log := &raft.Log{
			Index: 1,
			Term:  1,
			Type:  raft.LogCommand,
			Data:  payload,
		}

		encoded, err := codec.Encode(log)
		if err != nil {
			t.Fatalf("failed to encode: %v", err)
		}

		result, err := decoder.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if !bytes.Equal(result, payload) {
			t.Errorf("Decode() = %v, want %v", result, payload)
		}
	})

	t.Run("returns RaftInternal record for non-LogCommand types", func(t *testing.T) {
		appendedAt := time.Now()
		log := &raft.Log{
			Index:      42,
			Term:       1,
			Type:       raft.LogConfiguration,
			Data:       []byte("config data"),
			AppendedAt: appendedAt,
		}

		encoded, err := codec.Encode(log)
		if err != nil {
			t.Fatalf("failed to encode: %v", err)
		}

		result, err := decoder.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}

		if result == nil {
			t.Fatal("Decode() returned nil, want RaftInternal record")
		}

		fb := logrecord.GetRootAsLogRecord(result, 0)
		if fb.OperationType() != logrecord.LogOperationTypeRaftInternal {
			t.Errorf("OperationType = %v, want RaftInternal", fb.OperationType())
		}
		if fb.Lsn() != 42 {
			t.Errorf("LSN = %d, want 42", fb.Lsn())
		}
		if fb.Hlc() != uint64(appendedAt.UnixNano()) {
			t.Errorf("HLC = %d, want %d", fb.Hlc(), appendedAt.UnixNano())
		}
	})

	t.Run("handles empty input", func(t *testing.T) {
		result, err := decoder.Decode(nil)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		if result != nil {
			t.Errorf("Decode() = %v, want nil", result)
		}

		result, err = decoder.Decode([]byte{})
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		if result != nil {
			t.Errorf("Decode() = %v, want nil", result)
		}
	})

	t.Run("returns error for invalid data", func(t *testing.T) {
		_, err := decoder.Decode([]byte("invalid data"))
		if err == nil {
			t.Error("Decode() expected error for invalid data")
		}
	})
}

func TestNewRaftWALDecoder_DefaultCodec(t *testing.T) {
	decoder := NewRaftWALDecoder(nil)
	if decoder.codec == nil {
		t.Error("NewRaftWALDecoder(nil) should use default codec")
	}

	codec := raftwalfs.BinaryCodecV1{}
	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  []byte("test"),
	}

	encoded, err := codec.Encode(log)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	result, err := decoder.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if !bytes.Equal(result, []byte("test")) {
		t.Errorf("Decode() = %v, want %v", result, []byte("test"))
	}
}
