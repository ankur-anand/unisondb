package umetrics_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/pkg/umetrics"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally/v4"
)

type mockReporter struct {
	mu       sync.Mutex
	counters []reportedCounter
	gauges   []reportedGauge
}

type mockCounter struct {
	reporter *mockReporter
	name     string
	tags     map[string]string
}

func (c *mockCounter) ReportCount(value int64) {
	c.reporter.ReportCounter(c.name, c.tags, value)
}

func (r *mockReporter) AllocateCounter(name string, tags map[string]string) tally.CachedCount {
	return &mockCounter{
		reporter: r,
		name:     name,
		tags:     tags,
	}
}

type mockGauge struct {
	reporter *mockReporter
	name     string
	tags     map[string]string
}

func (g *mockGauge) ReportGauge(value float64) {
	g.reporter.ReportGauge(g.name, g.tags, value)
}

func (r *mockReporter) AllocateGauge(name string, tags map[string]string) tally.CachedGauge {
	return &mockGauge{
		reporter: r,
		name:     name,
		tags:     tags,
	}
}

func (r *mockReporter) AllocateTimer(name string, tags map[string]string) tally.CachedTimer {
	//TODO implement me
	panic("implement me")
}

func (r *mockReporter) AllocateHistogram(name string, tags map[string]string, buckets tally.Buckets) tally.CachedHistogram {
	//TODO implement me
	panic("implement me")
}

type reportedCounter struct {
	name  string
	tags  map[string]string
	value int64
}

type reportedGauge struct {
	name  string
	tags  map[string]string
	value float64
}

func (r *mockReporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = append(r.counters, reportedCounter{name, tags, value})
}

func (r *mockReporter) ReportGauge(name string, tags map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges = append(r.gauges, reportedGauge{name, tags, value})
}

func (r *mockReporter) ReportTimer(string, map[string]string, time.Duration) {}
func (r *mockReporter) Capabilities() tally.Capabilities                     { return r }
func (r *mockReporter) Reporting() bool                                      { return true }
func (r *mockReporter) Tagging() bool                                        { return true }
func (r *mockReporter) Flush()                                               {}

func (r *mockReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64,
) {
}

func (r *mockReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64,
) {
}

// We are running all the cases as subtest for reason we are testing Global
func TestUMetrics(t *testing.T) {
	var metricsName []string
	reporter := &mockReporter{}

	t.Run("scope_with_auto_skip", func(t *testing.T) {
		scope := callAutoScopeWithSkip(2)
		assert.NotNil(t, scope)
		assert.Contains(t, fmt.Sprintf("%+v", scope.SubScope("test")), "umetrics.test")
	})

	t.Run("custom_resolver", func(t *testing.T) {
		originalResolver := func() string {
			return "custom_test_scope"
		}
		umetrics.SetCustomScopeResolver(originalResolver)
		scope := umetrics.AutoScope()
		assert.Contains(t, fmt.Sprintf("%+v", scope.SubScope("test")), "custom_test_scope.test")
		umetrics.SetCustomScopeResolver(func() string {
			return "umetrics"
		})
	})

	t.Run("auto_scope", func(t *testing.T) {
		_, err := umetrics.Initialize(umetrics.Options{
			Prefix:         "testing",
			Reporter:       reporter,
			ReportInterval: 1,
			CommonTags:     map[string]string{"env": "test"},
		})
		assert.NoError(t, err)
		scope := umetrics.AutoScope()
		scope.Counter("calls").Inc(1)
		metricsName = append(metricsName, "testing_umetrics_calls")
		assert.Contains(t, fmt.Sprintf("%+v", scope.SubScope("t1")), "prefix:testing_umetrics_t1 tags:map[env:test]")
	})

	t.Run("get_scope_emits_expected_metric", func(t *testing.T) {
		scope := umetrics.GetScope("worker")
		scope.Counter("processed_total").Inc(1)

		time.Sleep(10 * time.Millisecond)

		reporter.mu.Lock()
		defer reporter.mu.Unlock()

		var found bool
		for _, c := range reporter.counters {
			if c.name == "testing_worker_processed_total" && c.value == 1 {
				found = true
			}
		}
		assert.True(t, found, "expected counter testing_worker_processed_total not reported")
	})

	t.Run("get_tagged_scope_applies_tags", func(t *testing.T) {
		scope := umetrics.GetTaggedScope("api", map[string]string{
			"region": "us-east-1",
			"tier":   "edge",
		})
		scope.Counter("requests_total").Inc(1)

		time.Sleep(10 * time.Millisecond)

		reporter.mu.Lock()
		defer reporter.mu.Unlock()

		var found bool
		for _, c := range reporter.counters {
			if c.name == "testing_api_requests_total" &&
				c.tags["region"] == "us-east-1" && c.tags["tier"] == "edge" {
				found = true
			}
		}

		assert.True(t, found, "tagged counter with expected labels not reported")
	})

	t.Run("startup_time_gauge_reported", func(t *testing.T) {
		time.Sleep(10 * time.Millisecond)

		reporter.mu.Lock()
		defer reporter.mu.Unlock()

		var found bool
		for _, g := range reporter.gauges {
			if g.name == "testing_process_start_time_seconds" && g.value > 0 {
				found = true
				break
			}
		}
		assert.True(t, found, "expected process_start_time_seconds not reported")
	})
	t.Run("initialize_is_idempotent", func(t *testing.T) {

		closer, err := umetrics.Initialize(umetrics.Options{
			Prefix:         "another_prefix",
			Reporter:       reporter,
			ReportInterval: 1,
			CommonTags:     map[string]string{"env": "should-not-affect"},
		})
		assert.NoError(t, err)
		assert.Nil(t, closer, "subsequent Initialize should return nil closer")
	})
}

func callAutoScopeWithSkip(skip int) tally.Scope {
	return umetrics.AutoScopeWithSkip(skip)
}
