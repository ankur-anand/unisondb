package walfs

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"testing"
)

type prefixStripDecoder struct {
	prefix []byte
}

func (d prefixStripDecoder) Decode(data []byte) ([]byte, error) {
	if len(data) < len(d.prefix) {
		return nil, errors.New("data too short")
	}
	if !bytes.Equal(data[:len(d.prefix)], d.prefix) {
		return nil, errors.New("prefix mismatch")
	}
	return data[len(d.prefix):], nil
}

func TestReader_WithCustomDecoder(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	wal, err := NewWALog(walDir, ".wal")
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	prefix := []byte("HDR:")
	payloads := []string{"hello", "world", "test"}

	for i, payload := range payloads {
		record := append(prefix, []byte(payload)...)
		_, err := wal.Write(record, uint64(i+1))
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}

	decoder := prefixStripDecoder{prefix: prefix}
	reader := wal.NewReader(WithDecoder(decoder))
	defer reader.Close()

	for i, expected := range payloads {
		data, _, err := reader.Next()
		if err != nil {
			t.Fatalf("record %d: Next() error = %v", i, err)
		}
		if string(data) != expected {
			t.Errorf("record %d: got %q, want %q", i, string(data), expected)
		}
	}

	_, _, err = reader.Next()
	if !errors.Is(err, io.EOF) && !errors.Is(err, ErrNoNewData) {
		t.Errorf("expected EOF or ErrNoNewData, got %v", err)
	}
}

func TestReader_DecoderError(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	wal, err := NewWALog(walDir, ".wal")
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	_, err = wal.Write([]byte("no prefix here"), 1)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	decoder := prefixStripDecoder{prefix: []byte("HDR:")}
	reader := wal.NewReader(WithDecoder(decoder))
	defer reader.Close()

	_, _, err = reader.Next()
	if err == nil {
		t.Error("expected decoder error, got nil")
	}
}

func TestNoopDecoder_Decode(t *testing.T) {
	decoder := NoopDecoder{}

	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "nil input",
			input: nil,
		},
		{
			name:  "empty input",
			input: []byte{},
		},
		{
			name:  "simple data",
			input: []byte("hello world"),
		},
		{
			name:  "binary data",
			input: []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decoder.Decode(tt.input)
			if err != nil {
				t.Errorf("Decode() error = %v, want nil", err)
			}
			if !bytes.Equal(result, tt.input) {
				t.Errorf("Decode() = %v, want %v", result, tt.input)
			}
		})
	}
}
