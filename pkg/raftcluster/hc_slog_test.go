package raftcluster

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

type logRecord struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
}

func newTestSlogLogger(buf *bytes.Buffer) *slog.Logger {
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(handler)
}

func readLastRecord(t *testing.T, buf *bytes.Buffer) logRecord {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[len(lines)-1] == "" {
		t.Fatalf("no log lines found")
	}

	var rec logRecord
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &rec); err != nil {
		t.Fatalf("unmarshal log record: %v", err)
	}
	return rec
}

func TestLWRWriteBracketLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestSlogLogger(&buf)
	writer := &lwr{logger: logger}

	tests := []struct {
		line      string
		wantLevel string
		wantMsg   string
	}{
		{"[DEBUG] debug msg", "DEBUG", "debug msg"},
		{"[INFO] info msg", "INFO", "info msg"},
		{"[WARN] warn msg", "WARN", "warn msg"},
		{"[ERROR] err msg", "ERROR", "err msg"},
		{"[UNKNOWN] unknown msg", "INFO", "unknown msg"},
	}

	for _, tt := range tests {
		buf.Reset()
		_, _ = writer.Write([]byte(tt.line))
		rec := readLastRecord(t, &buf)
		if rec.Level != tt.wantLevel {
			t.Fatalf("line %q: want level %s got %s", tt.line, tt.wantLevel, rec.Level)
		}
		if rec.Msg != tt.wantMsg {
			t.Fatalf("line %q: want msg %q got %q", tt.line, tt.wantMsg, rec.Msg)
		}
	}
}

func TestLWRWritePrefixLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestSlogLogger(&buf)
	writer := &lwr{logger: logger}

	tests := []struct {
		line      string
		wantLevel string
		wantMsg   string
	}{
		{"DEBUG debug msg", "DEBUG", "debug msg"},
		{"INFO info msg", "INFO", "info msg"},
		{"WARN warn msg", "WARN", "warn msg"},
		{"WARNING warn msg", "WARN", "warn msg"},
		{"ERROR err msg", "ERROR", "err msg"},
		{"ERR err msg", "ERROR", "err msg"},
		{"no level msg", "INFO", "no level msg"},
	}

	for _, tt := range tests {
		buf.Reset()
		_, _ = writer.Write([]byte(tt.line))
		rec := readLastRecord(t, &buf)
		if rec.Level != tt.wantLevel {
			t.Fatalf("line %q: want level %s got %s", tt.line, tt.wantLevel, rec.Level)
		}
		if rec.Msg != tt.wantMsg {
			t.Fatalf("line %q: want msg %q got %q", tt.line, tt.wantMsg, rec.Msg)
		}
	}
}

func TestLWRWriteEmptyLineNoOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestSlogLogger(&buf)
	writer := &lwr{logger: logger}

	_, _ = writer.Write([]byte("   "))
	if buf.Len() != 0 {
		t.Fatalf("expected no output for empty line")
	}
}

func TestLogLevelFromToken(t *testing.T) {
	tests := []struct {
		token string
		want  slog.Level
	}{
		{"DEBUG", slog.LevelDebug},
		{"WARN", slog.LevelWarn},
		{"WARNING", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"ERR", slog.LevelError},
		{"INFO", slog.LevelInfo},
		{"UNKNOWN", slog.LevelInfo},
	}

	for _, tt := range tests {
		got := logLevelFromToken(tt.token)
		if got != tt.want {
			t.Fatalf("token %q: want %v got %v", tt.token, tt.want, got)
		}
	}
}
