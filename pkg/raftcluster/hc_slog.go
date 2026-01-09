package raftcluster

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
)

type lwr struct {
	logger *slog.Logger
}

func (l *lwr) Write(p []byte) (int, error) {
	line := bytes.TrimSpace(p)
	if len(line) == 0 {
		return len(p), nil
	}

	level := slog.LevelInfo
	msg := line

	if start := bytes.IndexByte(line, '['); start >= 0 {
		if end := bytes.IndexByte(line[start:], ']'); end >= 0 {
			token := strings.ToUpper(string(line[start+1 : start+end]))
			level = logLevelFromToken(token)
			rest := bytes.TrimSpace(line[start+end+1:])
			if len(rest) > 0 {
				msg = rest
			}
		}
	} else {
		upper := strings.ToUpper(string(line))
		for _, entry := range []struct {
			prefix string
			level  slog.Level
		}{
			// matching is order dependent.
			{"WARNING", slog.LevelWarn},
			{"ERROR", slog.LevelError},
			{"DEBUG", slog.LevelDebug},
			{"INFO", slog.LevelInfo},
			{"WARN", slog.LevelWarn},
			{"ERR", slog.LevelError},
		} {
			if strings.HasPrefix(upper, entry.prefix) {
				level = entry.level
				rest := bytes.TrimSpace(line[len(entry.prefix):])
				if len(rest) > 0 {
					msg = rest
				}
				break
			}
		}
	}
	l.logger.Log(context.Background(), level, string(msg))
	return len(p), nil
}

func logLevelFromToken(token string) slog.Level {
	switch token {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR", "ERR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
