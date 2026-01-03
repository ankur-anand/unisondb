package cliapp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ankur-anand/unisondb/internal/services/fuzzer"
	"github.com/ankur-anand/unisondb/internal/services/relayer"
	"golang.org/x/sync/errgroup"
)

// FuzzerService runs the fuzzer for stress testing.
type FuzzerService struct {
	deps    *Dependencies
	enabled bool
}

func (f *FuzzerService) Name() string {
	return "fuzzer"
}

func (f *FuzzerService) Setup(ctx context.Context, deps *Dependencies) error {
	if deps.Mode != modeFuzzer {
		f.enabled = false
		return nil
	}
	f.enabled = true
	f.deps = deps

	if deps.Config.FuzzConfig.OpsPerNamespace == 0 || deps.Config.FuzzConfig.WorkersPerNamespace == 0 {
		return fmt.Errorf("invalid fuzz config: OpsPerNamespace=%d, WorkersPerNamespace=%d",
			deps.Config.FuzzConfig.OpsPerNamespace,
			deps.Config.FuzzConfig.WorkersPerNamespace,
		)
	}

	return nil
}

func (f *FuzzerService) Run(ctx context.Context) error {
	if !f.enabled {
		return nil
	}

	var delay time.Duration
	var err error
	if f.deps.Config.FuzzConfig.StartupDelay != "" {
		delay, err = time.ParseDuration(f.deps.Config.FuzzConfig.StartupDelay)
		if err != nil {
			return fmt.Errorf("invalid startup delay: %w", err)
		}
	}

	if delay > 0 {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "FUZZER.start.delayed"),
			slog.Duration("delay", delay))
		time.Sleep(delay)
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, engine := range f.deps.Engines {
		engine := engine
		g.Go(func() error {
			slog.Info("[unisondb.cliapp]",
				slog.String("event_type", "fuzzer.started"),
				slog.String("namespace", engine.Namespace()),
			)

			fuzzer.FuzzEngineOps(ctx,
				engine,
				f.deps.Config.FuzzConfig.OpsPerNamespace,
				f.deps.Config.FuzzConfig.WorkersPerNamespace,
				f.deps.FuzzStats,
				engine.Namespace(),
				f.deps.Config.FuzzConfig.EnableReadOps,
			)
			return nil
		})
	}

	g.Go(func() error {
		f.deps.FuzzStats.StartStatsMonitor(ctx, 1*time.Minute)
		return nil
	})

	if f.deps.Config.FuzzConfig.LocalRelayerCount > 0 {
		for _, engine := range f.deps.Engines {
			eng := engine
			g.Go(func() error {
				startTime := time.Now()
				hist, err := relayer.StartNLocalRelayer(ctx, eng, f.deps.Config.FuzzConfig.LocalRelayerCount, 1*time.Minute)
				if err != nil {
					return err
				}
				<-ctx.Done()
				relayer.ReportReplicationStats(hist, eng.Namespace(), startTime)
				return nil
			})
		}
	}

	return g.Wait()
}

func (f *FuzzerService) Close(ctx context.Context) error {
	return nil
}

func (f *FuzzerService) Enabled() bool {
	return f.enabled
}
