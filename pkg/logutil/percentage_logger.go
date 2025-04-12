package logutil

import (
	"context"
	"log/slog"
	"maps"
	"math/rand"
)

type PercentLogger struct {
	handler       slog.Handler
	levelPercents map[slog.Level]float64
	minLevel      slog.Level
}

func NewPercentLogger(levelPercents map[slog.Level]float64, handler slog.Handler, minLevel slog.Level) *slog.Logger {
	pl := &PercentLogger{
		handler:       handler,
		levelPercents: maps.Clone(levelPercents),
		minLevel:      minLevel,
	}
	return slog.New(pl)
}

func (pl *PercentLogger) Enabled(_ context.Context, level slog.Level) bool {
	if level < pl.minLevel {
		return false
	}

	percent, ok := pl.levelPercents[level]
	if !ok {
		return true
	}
	return rand.Float64()*100 < percent
}

func (pl *PercentLogger) Handle(ctx context.Context, r slog.Record) error {
	return pl.handler.Handle(ctx, r)
}

func (pl *PercentLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &PercentLogger{
		handler:       pl.handler.WithAttrs(attrs),
		levelPercents: pl.levelPercents,
		minLevel:      pl.minLevel,
	}
}

func (pl *PercentLogger) WithGroup(name string) slog.Handler {
	return &PercentLogger{
		handler:       pl.handler.WithGroup(name),
		levelPercents: pl.levelPercents,
		minLevel:      pl.minLevel,
	}
}
