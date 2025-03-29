package main

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

var (
	globalResult []byte
)

func TestMain(m *testing.M) {

	runtime.GOMAXPROCS(runtime.NumCPU())
	runtime.LockOSThread()

	result := m.Run()

	runtime.UnlockOSThread()
	os.Exit(result)
}

type benchConfig struct {
	name         string
	numGoroutine int
	timeout      time.Duration
}

func getConfigs() []benchConfig {
	return []benchConfig{
		{name: "config-100-1s", numGoroutine: 100, timeout: 1 * time.Second},
		{name: "config-1000-1s", numGoroutine: 1000, timeout: 1 * time.Second},
		{name: "config-5000-1s", numGoroutine: 5000, timeout: 1 * time.Second},
		{name: "config-10000-1s", numGoroutine: 10000, timeout: 1 * time.Second},
	}
}

// simulateWALIO simulates I/O operations returning some bytes
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

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
				var wg sync.WaitGroup
				var ready sync.WaitGroup
				wg.Add(cfg.numGoroutine)
				ready.Add(cfg.numGoroutine)

				errCh := make(chan error, cfg.numGoroutine)

				for j := 0; j < cfg.numGoroutine; j++ {
					go func() {
						defer wg.Done()
						mu.Lock()
						ready.Done()

						done := make(chan struct{})
						go func() {
							cond.Wait()
							close(done)
						}()

						select {
						case <-ctx.Done():
							mu.Unlock()
							errCh <- ctx.Err()
							return
						case <-done:
							result := simulateWALIO()
							globalResult = result
							mu.Unlock()
							errCh <- nil
							return
						}
					}()
				}

				ready.Wait()

				mu.Lock()
				cond.Broadcast()
				mu.Unlock()

				wg.Wait()
				close(errCh)

				timeouts := 0
				for err := range errCh {
					if errors.Is(err, context.DeadlineExceeded) {
						timeouts++
					}
				}

				cancel()
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
				ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
				ch := make(chan struct{}, 1)
				var wg sync.WaitGroup
				var ready sync.WaitGroup
				wg.Add(cfg.numGoroutine)
				ready.Add(cfg.numGoroutine)

				errCh := make(chan error, cfg.numGoroutine)

				for j := 0; j < cfg.numGoroutine; j++ {
					go func() {
						defer wg.Done()
						ready.Done()

						select {
						case <-ctx.Done():
							errCh <- ctx.Err()
							return
						case <-ch:
							result := simulateWALIO()
							globalResult = result
							errCh <- nil
						}
					}()
				}

				ready.Wait()

				for j := 0; j < cfg.numGoroutine; j++ {
					select {
					case <-ctx.Done():
						goto cleanup
					case ch <- struct{}{}:
					}
				}

			cleanup:

				wg.Wait()
				close(ch)
				close(errCh)

				timeouts := 0
				for err := range errCh {
					if errors.Is(err, context.DeadlineExceeded) {
						timeouts++
					}
				}

				cancel()
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
				ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
				ch := make(chan struct{})
				var wg sync.WaitGroup
				var ready sync.WaitGroup
				wg.Add(cfg.numGoroutine)
				ready.Add(cfg.numGoroutine)

				errCh := make(chan error, cfg.numGoroutine)

				for j := 0; j < cfg.numGoroutine; j++ {
					go func() {
						defer wg.Done()
						ready.Done()

						select {
						case <-ctx.Done():
							errCh <- ctx.Err()
							return
						case <-ch:
							result := simulateWALIO()
							globalResult = result
							errCh <- nil
						}
					}()
				}

				ready.Wait()

				select {
				case <-ctx.Done():
					close(ch)
				default:
					close(ch)
				}

				wg.Wait()
				close(errCh)

				timeouts := 0
				for err := range errCh {
					if errors.Is(err, context.DeadlineExceeded) {
						timeouts++
					}
				}
				
				cancel()
			}
		})
	}
}
