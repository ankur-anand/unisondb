package cliapp

import (
	"context"
	"log/slog"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
)

// OffsetLoggerService periodically logs engine offsets for monitoring.
type OffsetLoggerService struct {
	engines  map[string]*dbkernel.Engine
	interval time.Duration
}

func NewOffsetLoggerService(interval time.Duration) *OffsetLoggerService {
	return &OffsetLoggerService{interval: interval}
}

func (o *OffsetLoggerService) Name() string {
	return "offset-logger"
}

func (o *OffsetLoggerService) Setup(ctx context.Context, deps *Dependencies) error {
	o.engines = deps.Engines
	if o.interval == 0 {
		o.interval = 1 * time.Minute
	}
	return nil
}

func (o *OffsetLoggerService) Run(ctx context.Context) error {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			o.logOffsets()
		case <-ctx.Done():
			return nil
		}
	}
}

func (o *OffsetLoggerService) logOffsets() {
	for _, engine := range o.engines {
		currentOffset := engine.CurrentOffset()
		var segmentID uint32
		if currentOffset != nil {
			segmentID = currentOffset.SegmentID
		}

		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "engines.offset.report"),
			slog.Group("engine",
				slog.String("namespace", engine.Namespace()),
				slog.Uint64("lsn", engine.OpsReceivedCount()),
				slog.Uint64("current_segment_id", uint64(segmentID)),
			),
		)
	}
}

func (o *OffsetLoggerService) Close(ctx context.Context) error {
	return nil
}

// StreamAgeService periodically updates gRPC stream age metrics.
type StreamAgeService struct {
	statsHandler *grpcutils.GRPCStatsHandler
	interval     time.Duration
}

func NewStreamAgeService(interval time.Duration) *StreamAgeService {
	return &StreamAgeService{interval: interval}
}

func (s *StreamAgeService) Name() string {
	return "stream-age"
}

func (s *StreamAgeService) Setup(ctx context.Context, deps *Dependencies) error {
	s.statsHandler = deps.StatsHandler
	if s.interval == 0 {
		s.interval = 30 * time.Second
	}
	return nil
}

func (s *StreamAgeService) Run(ctx context.Context) error {
	if s.statsHandler == nil {
		return nil
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.statsHandler.UpdateStreamAgeBuckets()
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *StreamAgeService) Close(ctx context.Context) error {
	return nil
}
