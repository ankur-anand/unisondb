package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ankur-anand/unisondb/internal/services/fuzzer"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		baseURL       = flag.String("base-url", "http://localhost:4000/api/v1", "Base URL of the HTTP API prefix (e.g. http://host:8080/api/v1)")
		namespaceCSV  = flag.String("namespaces", "default", "Comma-separated list of namespaces to fuzz")
		opsPerSec     = flag.Int("ops", 200, "Target operations per second per namespace")
		workers       = flag.Int("workers", 8, "Number of concurrent workers per namespace")
		includeReads  = flag.Bool("read-ops", false, "Include read operations (GetKV/GetRowColumns)")
		statsAddr     = flag.String("stats-addr", "", "Optional address (e.g. :9090) to expose /stats JSON")
		statsInterval = flag.Duration("stats-log-interval", time.Minute, "How often to log stats locally; zero disables logging")
		runDuration   = flag.Duration("duration", 0, "Optional duration to run before exiting (0 = until signal)")
		panicOnIdle   = flag.Bool("panic-on-idle", false, "If true, panic when ops drop to zero (uses stats monitor)")
		monitorPeriod = flag.Duration("idle-check-interval", time.Minute, "Interval for idle detection when --panic-on-idle is set")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	namespaces := parseNamespaces(*namespaceCSV)
	if len(namespaces) == 0 {
		slog.Error("[httpfuzzer] no namespaces specified")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *runDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *runDuration)
		defer cancel()
	}

	availableNamespaces, err := fetchAvailableNamespaces(ctx, *baseURL)
	if err != nil {
		slog.Error("[httpfuzzer] failed to fetch health", slog.Any("error", err))
		os.Exit(1)
	}
	if err := ensureNamespaces(namespaces, availableNamespaces); err != nil {
		slog.Error("[httpfuzzer] namespace validation failed", slog.Any("error", err))
		os.Exit(1)
	}

	stats := fuzzer.NewFuzzStats()

	if *statsAddr != "" {
		go serveStats(ctx, *statsAddr, stats)
	}

	if *panicOnIdle {
		go stats.StartStatsMonitor(ctx, *monitorPeriod)
	} else if *statsInterval > 0 {
		go logStatsLoop(ctx, stats, *statsInterval)
	}

	slog.Info("[httpfuzzer] starting", slog.String("base_url", strings.TrimSuffix(*baseURL, "/")), slog.Int("ops_per_sec", *opsPerSec), slog.Int("workers", *workers), slog.Bool("read_ops", *includeReads), slog.Any("namespaces", namespaces))

	g, ctx := errgroup.WithContext(ctx)
	for _, ns := range namespaces {
		ns := ns
		g.Go(func() error {
			fuzzer.FuzzHTTPAPIOps(ctx, *baseURL, ns, *opsPerSec, *workers, stats, *includeReads)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		slog.Error("[httpfuzzer] exiting", slog.Any("error", err))
	}

	snapshot := stats.Snapshot()
	for ns, nsStats := range snapshot {
		slog.Info("[httpfuzzer] summary",
			slog.String("namespace", ns),
			slog.Any("ops", nsStats.OpCount),
			slog.Int64("errors", nsStats.ErrorCount),
			slog.Float64("ops_rate", nsStats.OpsRate),
			slog.Any("latency_ms", nsStats.Latency),
		)
	}
	printStatsCSV(snapshot)
}

func parseNamespaces(csv string) []string {
	parts := strings.Split(csv, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func serveStats(ctx context.Context, addr string, stats *fuzzer.FuzzStats) {
	srv := &http.Server{Addr: addr, Handler: stats}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("[httpfuzzer] stats server failed", slog.Any("error", err))
	}
}

func logStatsLoop(ctx context.Context, stats *fuzzer.FuzzStats, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := stats.Snapshot()
			for ns, nsStats := range snapshot {
				slog.Info("[httpfuzzer] stats",
					slog.String("namespace", ns),
					slog.Any("ops", nsStats.OpCount),
					slog.Int64("errors", nsStats.ErrorCount),
					slog.Float64("ops_rate", nsStats.OpsRate),
					slog.Any("latency_ms", nsStats.Latency),
				)
			}
		}
	}
}

func fetchAvailableNamespaces(ctx context.Context, baseURL string) ([]string, error) {
	trimmed := strings.TrimSuffix(baseURL, "/")
	u, err := url.Parse(trimmed)
	if err != nil {
		return nil, err
	}
	u.Path = "/healthz"
	u.RawQuery = ""
	u.Fragment = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("health request failed: status %d", resp.StatusCode)
	}
	var payload struct {
		Status     string   `json:"status"`
		Namespaces []string `json:"namespaces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode health response: %w", err)
	}
	return payload.Namespaces, nil
}

func ensureNamespaces(requested, available []string) error {
	set := make(map[string]struct{}, len(available))
	for _, ns := range available {
		set[ns] = struct{}{}
	}
	var missing []string
	for _, ns := range requested {
		if _, ok := set[ns]; !ok {
			missing = append(missing, ns)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("namespaces not found on server: %s", strings.Join(missing, ","))
	}
	return nil
}

func printStatsCSV(snapshot map[string]fuzzer.NamespaceStats) {
	fmt.Println("timestamp,namespace,ops_rate,throughput,p50_ms,p90_ms,p99_ms,error_count,total_ops")
	now := time.Now().Format(time.RFC3339)
	for ns, nsStats := range snapshot {
		var totalOps int64
		for _, count := range nsStats.OpCount {
			totalOps += count
		}
		lat := nsStats.Latency
		fmt.Printf("%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d\n",
			now,
			ns,
			nsStats.OpsRate,
			nsStats.Throughput,
			lat["p50"],
			lat["p90"],
			lat["p99"],
			nsStats.ErrorCount,
			totalOps,
		)
	}
}
