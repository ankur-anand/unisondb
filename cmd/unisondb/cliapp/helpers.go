package cliapp

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
)

// clampInt64 clamps a value between min and max.
func clampInt64(value, min, max int64) int64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

// clampToUint32 clamps a value to uint32 range with a minimum.
func clampToUint32(value int64, min uint32) uint32 {
	if value < 0 {
		return min
	}
	if value > int64(math.MaxUint32) {
		return math.MaxUint32
	}
	if value < int64(min) {
		return min
	}
	return uint32(value)
}

func logClamped[T comparable](field string, original, clamped T) {
	if original != clamped {
		slog.Warn("[unisondb.cliapp] Storage configuration value clamped",
			"field", field,
			"original", original,
			"clamped", clamped,
		)
	}
}

func parseLogLevel(levelStr string) (slog.Level, error) {
	levelStr = strings.TrimSpace(levelStr)
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "":
		return slog.LevelInfo, nil
	default:
		return 0, fmt.Errorf("unknown log level: %q", levelStr)
	}
}

// isValidPort checks if a given integer is a valid port number (1-65535).
func isValidPort(port int) bool {
	return port >= 1 && port <= 65535
}
