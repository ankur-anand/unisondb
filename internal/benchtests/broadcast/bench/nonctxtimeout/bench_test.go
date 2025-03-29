package nonctxtimeout

import (
	"context"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
)

func TestMain(m *testing.M) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	runtime.LockOSThread()

	result := m.Run()

	runtime.UnlockOSThread()
	os.Exit(result)
}

var (
	globalResult []byte
)

type benchConfig struct {
	name         string
	numGoroutine int
}

// getConfigs returns the benchmark configurations
func getConfigs() []benchConfig {
	configs := []benchConfig{
		{name: "config-100", numGoroutine: 100},
		{name: "config-1000", numGoroutine: 1000},
		{name: "config-5000", numGoroutine: 5000},
		{name: "config-10000", numGoroutine: 10000},
	}
	return configs
}

func simulateWALIO() []byte {
	data := make([]byte, 36) // UUID length
	copy(data, gofakeit.UUID())
	return data
}

func BenchmarkSyncCond(b *testing.B) {
	configs := getConfigs()
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			var mu sync.Mutex
			cond := sync.NewCond(&mu)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				var ready sync.WaitGroup
				wg.Add(cfg.numGoroutine)
				ready.Add(cfg.numGoroutine)

				for j := 0; j < cfg.numGoroutine; j++ {
					go func() {
						defer wg.Done()
						mu.Lock()
						ready.Done()
						for {
							select {
							case <-ctx.Done():
								mu.Unlock()
								return
							default:
								cond.Wait()
								result := simulateWALIO()
								globalResult = result
								mu.Unlock()
								return
							}
						}
					}()
				}

				ready.Wait()

				mu.Lock()
				cond.Broadcast()
				mu.Unlock()

				wg.Wait()
			}
		})
	}
}

func BenchmarkBufferedChannel(b *testing.B) {
	configs := getConfigs()
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				ch := make(chan struct{}, 1)
				var wg sync.WaitGroup
				var ready sync.WaitGroup
				wg.Add(cfg.numGoroutine)
				ready.Add(cfg.numGoroutine)

				for j := 0; j < cfg.numGoroutine; j++ {
					go func() {
						defer wg.Done()
						ready.Done()
						<-ch
						result := simulateWALIO()
						globalResult = result
					}()
				}

				ready.Wait()

				for j := 0; j < cfg.numGoroutine; j++ {
					ch <- struct{}{}
				}

				wg.Wait()
				close(ch)
			}
		})
	}
}

func BenchmarkUnbufferedChannel(b *testing.B) {
	configs := getConfigs()
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				ch := make(chan struct{})
				var wg sync.WaitGroup
				var ready sync.WaitGroup
				wg.Add(cfg.numGoroutine)
				ready.Add(cfg.numGoroutine)

				for j := 0; j < cfg.numGoroutine; j++ {
					go func() {
						defer wg.Done()
						ready.Done()
						<-ch
						result := simulateWALIO()
						globalResult = result
					}()
				}

				ready.Wait()

				for j := 0; j < cfg.numGoroutine; j++ {
					ch <- struct{}{}
				}

				wg.Wait()
				close(ch)
			}
		})
	}
}

func BenchmarkChannelCloseBroadcast(b *testing.B) {
	configs := getConfigs()
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				ch := make(chan struct{})
				var wg sync.WaitGroup
				var ready sync.WaitGroup
				wg.Add(cfg.numGoroutine)
				ready.Add(cfg.numGoroutine)

				for j := 0; j < cfg.numGoroutine; j++ {
					go func() {
						defer wg.Done()
						ready.Done()
						<-ch
						result := simulateWALIO()
						globalResult = result
					}()
				}

				ready.Wait()

				close(ch)

				wg.Wait()
			}
		})
	}
}
