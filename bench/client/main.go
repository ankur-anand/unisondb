package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/unisondb/internal/services/fuzzer"
	"github.com/dustin/go-humanize"
	llhist "github.com/openhistogram/circonusllhist"
	"github.com/prometheus/common/helpers/templates"
)

type benchResult struct {
	SizeBytes int
	Ops       int64
	Errors    int64
	OpsPerSec float64
	P50Ms     float64
	P95Ms     float64
	P99Ms     float64
	Duration  time.Duration
}

const (
	stageWarmup  int32 = 0
	stageMeasure int32 = 1
	stageDone    int32 = 2
)

type benchStats struct {
	ops    atomic.Int64
	errors atomic.Int64
	mu     sync.Mutex
	hist   *llhist.Histogram
}

func newBenchStats() *benchStats {
	return &benchStats{hist: llhist.New()}
}

func (s *benchStats) Reset() {
	s.ops.Store(0)
	s.errors.Store(0)
	s.mu.Lock()
	s.hist = llhist.New()
	s.mu.Unlock()
}

func (s *benchStats) Record(d time.Duration, err error) {
	s.ops.Add(1)
	if err != nil {
		s.errors.Add(1)
	}
	s.mu.Lock()
	_ = s.hist.RecordValue(d.Seconds())
	s.mu.Unlock()
}

func (s *benchStats) Snapshot(size int, elapsed time.Duration) benchResult {
	ops := s.ops.Load()
	ers := s.errors.Load()
	var p50, p95, p99 float64
	s.mu.Lock()
	p50 = s.hist.ValueAtQuantile(0.50) * 1000
	p95 = s.hist.ValueAtQuantile(0.95) * 1000
	p99 = s.hist.ValueAtQuantile(0.99) * 1000
	s.mu.Unlock()
	opsPerSec := 0.0
	if elapsed > 0 {
		opsPerSec = float64(ops) / elapsed.Seconds()
	}
	return benchResult{
		SizeBytes: size,
		Ops:       ops,
		Errors:    ers,
		OpsPerSec: opsPerSec,
		P50Ms:     p50,
		P95Ms:     p95,
		P99Ms:     p99,
		Duration:  elapsed,
	}
}

func main() {
	var (
		baseURL   = flag.String("base-url", "http://127.0.0.1:4001/api/v1", "HTTP API base URL of the leader (e.g. http://host:port/api/v1)")
		namespace = flag.String("namespace", "default", "Namespace to target")
		workers   = flag.Int("workers", 32, "Concurrent workers")
		warmup    = flag.Duration("warmup", 30*time.Second, "Warmup duration")
		duration  = flag.Duration("duration", 2*time.Minute, "Measurement duration")
		sizesCSV  = flag.String("sizes", "256,512,1024", "Comma-separated value sizes in bytes")
		keyPrefix = flag.String("key-prefix", "bench", "Key prefix to avoid collisions")
		timeout   = flag.Duration("timeout", 10*time.Second, "HTTP client timeout")
		progress  = flag.Duration("progress", 10*time.Second, "Progress log interval (0 to disable)")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if *workers < 1 {
		slog.Error("[bench-client] workers must be >= 1")
		os.Exit(1)
	}
	if *duration <= 0 {
		slog.Error("[bench-client] duration must be > 0")
		os.Exit(1)
	}

	sizes, err := parseSizes(*sizesCSV)
	if err != nil {
		slog.Error("[bench-client] invalid sizes", slog.Any("error", err))
		os.Exit(1)
	}

	httpClient := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *workers * 2,
			MaxConnsPerHost:     *workers * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	engine := fuzzer.NewHTTPAPIEngine(context.Background(), *baseURL, *namespace, httpClient)

	slog.Info("[bench-client] starting",
		slog.String("base_url", strings.TrimSuffix(*baseURL, "/")),
		slog.String("namespace", *namespace),
		slog.Int("workers", *workers),
		slog.String("warmup", humanizeDuration(*warmup)),
		slog.String("duration", humanizeDuration(*duration)),
		slog.Any("sizes", sizes),
	)

	var results []benchResult
	for _, size := range sizes {
		result := runBenchmark(engine, size, *workers, *warmup, *duration, *keyPrefix, *progress)
		results = append(results, result)
		slog.Info("[bench-client] result",
			slog.Int("size_bytes", result.SizeBytes),
			slog.String("total_ops", formatCountSI(float64(result.Ops), 2)),
			slog.String("errors", formatCountSI(float64(result.Errors), 2)),
			slog.String("ops_per_sec", formatCountSI(result.OpsPerSec, 2)),
			slog.String("p50_ms", formatLatencyMs(result.P50Ms, 2)),
			slog.String("p95_ms", formatLatencyMs(result.P95Ms, 2)),
			slog.String("p99_ms", formatLatencyMs(result.P99Ms, 2)),
			slog.String("duration", humanizeDuration(result.Duration)),
		)
	}

	fmt.Println("size_bytes,ops,errors,ops_per_sec,p50_ms,p95_ms,p99_ms,duration")
	for _, r := range results {
		fmt.Printf("%d,%s,%s,%s,%s,%s,%s,%s\n",
			r.SizeBytes,
			formatCountSI(float64(r.Ops), 2),
			formatCountSI(float64(r.Errors), 2),
			formatCountSI(r.OpsPerSec, 2),
			formatLatencyMs(r.P50Ms, 2),
			formatLatencyMs(r.P95Ms, 2),
			formatLatencyMs(r.P99Ms, 2),
			humanizeDuration(r.Duration),
		)
	}
}

func runBenchmark(engine *fuzzer.HTTPAPIEngine, size int, workers int, warmup time.Duration, duration time.Duration, keyPrefix string, progress time.Duration) benchResult {
	value := make([]byte, size)
	fillRandomBytes(value)

	stats := newBenchStats()
	var record atomic.Bool
	var stage atomic.Int32
	var warmupStart atomic.Int64
	var measureStart atomic.Int64

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	if progress > 0 {
		warmupStart.Store(time.Now().UnixNano())
		stage.Store(stageWarmup)
		startProgressLogger(ctx, size, warmup, progress, stats, &stage, &warmupStart, &measureStart)
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, engine, stats, &record, keyPrefix, workerID, value)
		}(w)
	}

	if warmup > 0 {
		slog.Info("[bench-client] warmup starting",
			slog.Int("size_bytes", size),
			slog.String("duration", humanizeDuration(warmup)),
		)
		warmupStart.Store(time.Now().UnixNano())
		stage.Store(stageWarmup)
		time.Sleep(warmup)
	}

	stats.Reset()
	record.Store(true)
	measureStart.Store(time.Now().UnixNano())
	stage.Store(stageMeasure)
	slog.Info("[bench-client] measurement starting",
		slog.Int("size_bytes", size),
		slog.String("duration", humanizeDuration(duration)),
	)
	start := time.Now()
	time.Sleep(duration)
	record.Store(false)
	stage.Store(stageDone)
	cancel()
	wg.Wait()

	return stats.Snapshot(size, time.Since(start))
}

func runWorker(ctx context.Context, engine *fuzzer.HTTPAPIEngine, stats *benchStats, record *atomic.Bool,
	keyPrefix string, workerID int, value []byte,
) {
	var counter uint64
	keyBuf := make([]byte, 0, len(keyPrefix)+32)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		keyBuf = appendKey(keyBuf[:0], keyPrefix, workerID, counter)
		start := time.Now()
		err := engine.PutKV(keyBuf, value)
		if err != nil {
			slog.Error("[bench-client] put kv error", "err", err)
		}
		if record.Load() {
			stats.Record(time.Since(start), err)
		}
		counter++
	}
}

func appendKey(buf []byte, prefix string, workerID int, counter uint64) []byte {
	buf = append(buf, prefix...)
	buf = append(buf, '-')
	buf = strconv.AppendInt(buf, int64(workerID), 10)
	buf = append(buf, '-')
	buf = strconv.AppendUint(buf, counter, 10)
	return buf
}

func fillRandomBytes(b []byte) {
	for i := range b {
		b[i] = byte(rand.IntN(256))
	}
}

func parseSizes(csv string) ([]int, error) {
	parts := strings.Split(csv, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		size, err := strconv.Atoi(part)
		if err != nil || size <= 0 {
			return nil, fmt.Errorf("invalid size %q", part)
		}
		out = append(out, size)
	}
	if len(out) == 0 {
		return nil, errors.New("no sizes provided")
	}
	return out, nil
}

func startProgressLogger(ctx context.Context, size int, warmup time.Duration, interval time.Duration, stats *benchStats,
	stage *atomic.Int32, warmupStart *atomic.Int64, measureStart *atomic.Int64,
) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				switch stage.Load() {
				case stageWarmup:
					if warmup <= 0 {
						continue
					}
					start := time.Unix(0, warmupStart.Load())
					elapsed := time.Since(start)
					remaining := warmup - elapsed
					if remaining < 0 {
						remaining = 0
					}
					slog.Info("[bench-client] warmup",
						slog.Int("size_bytes", size),
						slog.String("elapsed", humanizeDuration(elapsed)),
						slog.String("remaining", humanizeDuration(remaining)),
					)
				case stageMeasure:
					start := time.Unix(0, measureStart.Load())
					elapsed := time.Since(start)
					ops := stats.ops.Load()
					ers := stats.errors.Load()
					opsPerSec := 0.0
					if elapsed > 0 {
						opsPerSec = float64(ops) / elapsed.Seconds()
					}
					slog.Info("[bench-client] progress",
						slog.Int("size_bytes", size),
						slog.String("elapsed", humanizeDuration(elapsed)),
						slog.String("total_ops", formatCountSI(float64(ops), 2)),
						slog.String("ers", formatCountSI(float64(ers), 2)),
						slog.String("ops_per_sec", formatCountSI(opsPerSec, 2)),
					)
				default:
				}
			}
		}
	}()
}

func formatCountSI(value float64, decimals int) string {
	if value == 0 {
		return "0"
	}
	scaled, prefix := humanize.ComputeSI(value)
	return humanize.FtoaWithDigits(scaled, decimals) + prefix
}

func formatLatencyMs(value float64, decimals int) string {
	if value == 0 {
		return "0"
	}
	return humanize.FtoaWithDigits(value, decimals)
}

func humanizeDuration(d time.Duration) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		return d.Truncate(time.Second).String()
	}
	return strings.ReplaceAll(s, " ", "")
}
