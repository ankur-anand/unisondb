package umetrics

import (
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
)

var (
	globalRegistry *Registry
	once           sync.Once
	lOnce          sync.Once
)

// Registry holds the global metrics configuration.
type Registry struct {
	scope      tally.Scope
	commonTags map[string]string
}

// Options for configuring the metrics registry.
type Options struct {
	Prefix         string
	Reporter       tally.CachedStatsReporter
	ReportInterval time.Duration
	CommonTags     map[string]string
	InitTime       time.Time
}

// Initialize the global metrics registry.
func Initialize(opts Options) (io.Closer, error) {
	var closer io.Closer
	var err error

	if globalRegistry != nil {
		return nil, nil
	}

	if opts.InitTime.IsZero() {
		opts.InitTime = time.Now().UTC()
	}

	once.Do(func() {
		if opts.CommonTags == nil {
			opts.CommonTags = make(map[string]string)
		}

		scope, scopeCloser := tally.NewRootScope(tally.ScopeOptions{
			Prefix:         opts.Prefix,
			Tags:           opts.CommonTags,
			CachedReporter: opts.Reporter,
			Separator:      "_",
		}, opts.ReportInterval)

		scope.Gauge("process_start_time_seconds").Update(float64(opts.InitTime.Unix()))
		globalRegistry = &Registry{
			scope:      scope,
			commonTags: opts.CommonTags,
		}
		closer = scopeCloser
	})

	return closer, err
}

// GetScope returns a scoped metrics collector for a specific package.
// nolint:ireturn
func GetScope(packageName string) tally.Scope {
	reg := globalRegistry
	if reg == nil {
		lOnce.Do(func() {
			slog.Warn("[unsiondb.umetrics] globalRegistry is nil")
		})
		return tally.NoopScope.SubScope(packageName)
	}
	return reg.scope.SubScope(packageName)
}

// GetTaggedScope returns a scoped metrics collector with additional tags.
// nolint:ireturn
func GetTaggedScope(packageName string, tags map[string]string) tally.Scope {
	return GetScope(packageName).Tagged(tags)
}
