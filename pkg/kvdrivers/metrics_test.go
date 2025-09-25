package kvdrivers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally/v4"
)

func TestMetricsTracker_RecordOp(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	tracker := NewScopedMetricsTracker(scope, "lmdb", "tenant_1")

	start := time.Now().Add(-10 * time.Millisecond)
	tracker.RecordOp("set", start)

	snap := scope.Snapshot()
	key := "test.ops_latency+db=lmdb,namespace=tenant_1,op=set"
	vals := snap.Timers()[key].Values()
	assert.Len(t, vals, 1)
	assert.GreaterOrEqual(t, vals[0], 10*time.Millisecond)

	assert.Equal(t, int64(1), snap.Counters()["test.ops_total+db=lmdb,namespace=tenant_1,op=set"].Value())
}

func TestMetricsTracker_RecordFlush(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	tracker := NewScopedMetricsTracker(scope, "bolt", "shard_5")

	start := time.Now().Add(-5 * time.Millisecond)
	tracker.RecordFlush(42, start)

	snap := scope.Snapshot()

	assert.Equal(t, int64(1), snap.Counters()["test.flush_total+db=bolt,namespace=shard_5"].Value())

	timerVals := snap.Timers()["test.flush_latency+db=bolt,namespace=shard_5"].Values()
	assert.Len(t, timerVals, 1)
	assert.GreaterOrEqual(t, timerVals[0], 5*time.Millisecond)

	hist := snap.Histograms()["test.flush_batch_size+db=bolt,namespace=shard_5"]
	assert.Nil(t, hist)
}

func TestMetricsTracker_RecordSnapshot(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	tracker := NewScopedMetricsTracker(scope, "bolt", "snap_test")

	start := time.Now().Add(-15 * time.Millisecond)
	tracker.RecordSnapshot(start)

	snap := scope.Snapshot()

	assert.Equal(t, int64(1), snap.Counters()["test.snapshot_total+db=bolt,namespace=snap_test"].Value())

	timerVals := snap.Timers()["test.snapshot_latency+db=bolt,namespace=snap_test"].Values()
	assert.Len(t, timerVals, 1)
	assert.GreaterOrEqual(t, timerVals[0], 15*time.Millisecond)
}

func TestMetricsTracker_RecordUnknownOp(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	tracker := NewScopedMetricsTracker(scope, "lmdb", "bad_op")

	start := time.Now().Add(-1 * time.Millisecond)
	tracker.RecordOp("unknown", start)

	snap := scope.Snapshot()
	assert.Empty(t, snap.Counters())
	assert.Empty(t, snap.Timers())
}

func TestMetricsTracker_RecordError(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	tracker := &MetricsTracker{
		opScopes: map[string]tally.Scope{
			"set": scope.Tagged(map[string]string{"op": "set"}),
		},
		root: scope,
	}

	tracker.RecordError("set")

	snapshot := scope.Snapshot()
	counter := snapshot.Counters()["test.ops_error_total+op=set"]
	assert.NotNil(t, counter)
	assert.Equal(t, int64(1), counter.Value())
}
