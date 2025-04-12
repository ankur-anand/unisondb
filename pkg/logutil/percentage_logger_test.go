package logutil_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/ankur-anand/unisondb/pkg/logutil"
)

func newTestLogger(buf *bytes.Buffer, percent map[slog.Level]float64, min slog.Level) *slog.Logger {
	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return logutil.NewPercentLogger(percent, handler, min)
}

func Test_MinLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, nil, slog.LevelInfo)

	logger.Debug("should not log")
	logger.Info("should log")

	logs := buf.String()
	if strings.Contains(logs, "should not log") {
		t.Error("Expected debug message to be skipped due to minLevel")
	}
	if !strings.Contains(logs, "should log") {
		t.Error("Expected info message to be logged")
	}
}

func Test_Sampling100Percent(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, map[slog.Level]float64{
		slog.LevelInfo: 100.0,
	}, slog.LevelDebug)

	logger.Info("always log")
	if !strings.Contains(buf.String(), "always log") {
		t.Error("Expected message to be logged at 100% sampling")
	}
}

func Test_Sampling25Percent(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, map[slog.Level]float64{
		slog.LevelInfo: 25.0,
	}, slog.LevelDebug)

	// Run multiple times to verify that at least one is skipped
	count := 0
	for i := 0; i < 100; i++ {
		buf.Reset()
		logger.Info("maybe log")
		if strings.Contains(buf.String(), "maybe log") {
			count++
		}
	}
	// we took some baseline greater as random number can't be perfect.
	if count == 0 || count >= 40 {
		t.Errorf("Expected some, but not all logs to appear; got %d/100", count)
	}
}

func Test_LogWithoutSamplingRule(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, map[slog.Level]float64{}, slog.LevelDebug)

	logger.Warn("no rule but still log")
	if !strings.Contains(buf.String(), "no rule but still log") {
		t.Error("Expected warn message to be logged without sampling rule")
	}
}

func Test_WithAttrsPreservesSampling(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := newTestLogger(&buf, map[slog.Level]float64{
		slog.LevelInfo: 100.0,
	}, slog.LevelDebug)

	withAttr := baseLogger.With("scope", "test")
	withAttr.Info("attribute log")

	baseLogger.Info("hi")
	logs := buf.String()
	if !strings.Contains(logs, "attribute log") || !strings.Contains(logs, "scope=test") {
		t.Error("Expected attribute log with 'scope=test' to be logged")
	}
}

func Test_WithGroupPreservesSamplingAndPrefix(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := newTestLogger(&buf, map[slog.Level]float64{
		slog.LevelInfo: 100.0,
	}, slog.LevelDebug)

	grouped := baseLogger.WithGroup("http")
	grouped.Info("grouped log", "method", "GET", "status", 200)

	logs := buf.String()
	if !strings.Contains(logs, "grouped log") {
		t.Error("Expected grouped log message to be present")
	}
	if !strings.Contains(logs, "http.method=GET") || !strings.Contains(logs, "http.status=200") {
		t.Error("Expected attributes to be grouped under 'http'")
	}
}
