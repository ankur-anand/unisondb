package util

import (
	"context"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/relayer"
	llhist "github.com/openhistogram/circonusllhist"
)

// StartNLocalRelayer simply delegates to the internal relayer entry point.
func StartNLocalRelayer(ctx context.Context, engine *dbkernel.Engine, num int, metricsTickInterval time.Duration) ([]*llhist.Histogram, error) {
	return relayer.StartNLocalRelayer(ctx, engine, num, metricsTickInterval)
}

// ReportReplicationStats exposes the internal stats helper for callers outside this module tree.
func ReportReplicationStats(hists []*llhist.Histogram, namespace string, start time.Time) {
	relayer.ReportReplicationStats(hists, namespace, start)
}
