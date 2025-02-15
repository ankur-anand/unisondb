package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ankur-anand/kvalchemy/internal/benchmark/genclient/gen"
	"github.com/openhistogram/circonusllhist"
	"github.com/prometheus/common/helpers/templates"
	"github.com/redis/go-redis/v9"
	"github.com/rosedblabs/wal"
)

var (
	hostName       = flag.String("h", "127.0.0.1", "host name")
	port           = flag.Int("p", 6380, "port number")
	concurrency    = flag.Int("c", 50, "concurrency")
	totalReq       = flag.Int64("n", 100000, "total requests")
	dataSize       = flag.Uint("d", 1*wal.KB, "data size in bytes")
	setGetOpsRatio = flag.Float64("r", 0.7, "set operations ratio")
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	flag.Parse()
	addr := fmt.Sprintf("%s:%d", *hostName, *port)
	if *setGetOpsRatio < 0.5 {
		panic("invalid set operations ratio must be equal to or greater than 0.5")
	}

	rClient := redis.NewClient(&redis.Options{
		Addr:           addr,
		MaxActiveConns: *concurrency,
		PoolSize:       *concurrency,
	})
	cmd := rClient.Ping(ctx)
	if cmd.Err() != nil {
		log.Fatal(cmd.Err())
	}

	redisGSI := gen.NewRedisGen(ctx, rClient, *dataSize)
	setOps := int64(*setGetOpsRatio * float64(*totalReq))
	getOps := *totalReq - setOps

	generator := gen.NewRatioRobin(redisGSI, redisGSI, uint64(setOps), uint64(getOps))

	taskPerWorker := int64(math.Ceil(float64(*totalReq) / float64(*concurrency)))

	var wg sync.WaitGroup
	h := circonusllhist.New()
	startTime := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for taskIndex := int64(0); taskIndex < taskPerWorker; taskIndex++ {
				start := time.Now()
				select {
				case <-ctx.Done():
					return
				default:
					generator.Gen()
					h.RecordDuration(time.Since(start))
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	<-ctx.Done()
	elapsedTime := time.Since(startTime).Seconds()
	requestsPerSecond := math.Ceil(float64(*totalReq) / elapsedTime)

	log.Printf("OPS: rps=%d, p50=%s, p75=%s, p99=%s", int(requestsPerSecond),
		humanizeDuration(h.ApproxMean()), humanizeDuration(h.ValueAtQuantile(.75)),
		humanizeDuration(h.ValueAtQuantile(.99)))
	p50, p75, p99 := humanizePercentile(redisGSI.SetLatency)
	log.Printf("SET: count=%d, p50=%s, p75=%s, p99=%s", redisGSI.WriteIndex.Load(), p50, p75, p99)
	p50, p75, p99 = humanizePercentile(redisGSI.GetLatency)
	log.Printf("GET: count=%d, p50=%s, p75=%s, p99=%s", redisGSI.ReadIndex.Load(), p50, p75, p99)
}

func humanizePercentile(h *circonusllhist.Histogram) (string, string, string) {
	return humanizeDuration(h.ApproxMean()), humanizeDuration(h.ValueAtQuantile(.75)),
		humanizeDuration(h.ValueAtQuantile(.99))
}

func humanizeDuration(d float64) string {
	s, err := templates.HumanizeDuration(d)
	if err != nil {
		log.Fatal(err)
	}
	return s
}
