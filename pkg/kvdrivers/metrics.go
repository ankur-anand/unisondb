package kvdrivers

import (
	"time"

	"github.com/ankur-anand/unisondb/pkg/umetrics"
	"github.com/uber-go/tally/v4"
)

// MetricsTracker provides a structured way to record operational metrics.
type MetricsTracker struct {
	opScopes map[string]tally.Scope
	root     tally.Scope
}

// NewMetricsTracker creates a MetricsTracker using the default Tally scope from umetrics.AutoScope.
func NewMetricsTracker(db, namespace string) *MetricsTracker {
	return NewScopedMetricsTracker(umetrics.AutoScope(), db, namespace)
}

// NewScopedMetricsTracker creates a MetricsTracker using a provided tally.Scope.
func NewScopedMetricsTracker(scope tally.Scope, db, namespace string) *MetricsTracker {
	baseTags := map[string]string{"db": db, "namespace": namespace}
	baseScope := scope.Tagged(baseTags)

	opScopes := map[string]tally.Scope{
		"get":     baseScope.Tagged(map[string]string{"op": "get"}),
		"set":     baseScope.Tagged(map[string]string{"op": "set"}),
		"delete":  baseScope.Tagged(map[string]string{"op": "delete"}),
		"get_row": baseScope.Tagged(map[string]string{"op": "get_row"}),
	}

	return &MetricsTracker{
		opScopes: opScopes,
		root:     baseScope,
	}
}

// RecordOp logs operation count and latency.
func (m *MetricsTracker) RecordOp(op string, start time.Time) {
	if s, ok := m.opScopes[op]; ok {
		s.Counter("ops_total").Inc(1)
		s.Timer("ops_latency").Record(time.Since(start))
	}
}

// RecordFlush logs flush-related stats.
func (m *MetricsTracker) RecordFlush(batchSize int, start time.Time) {
	m.root.Counter("flush_total").Inc(1)
	m.root.Timer("flush_latency").Record(time.Since(start))
}

// RecordSnapshot logs snapshot metrics.
func (m *MetricsTracker) RecordSnapshot(start time.Time) {
	m.root.Counter("snapshot_total").Inc(1)
	m.root.Timer("snapshot_latency").Record(time.Since(start))
}

// RecordError increments the error counter for a given operation (e.g., set, get, delete).
func (m *MetricsTracker) RecordError(op string) {
	if s, ok := m.opScopes[op]; ok {
		s.Counter("ops_error_total").Inc(1)
	}
}

func (m *MetricsTracker) RecordBatchOps(op string, count int) {
	if s, ok := m.opScopes[op]; ok {
		s.Counter("ops_total").Inc(int64(count))
	}
}

func (m *MetricsTracker) RecordWriteUnits(n int) {
	m.root.Counter("entries_modified_total").Inc(int64(n))
}
