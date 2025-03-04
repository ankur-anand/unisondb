package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

const (
	csvFile    = "benchmark_results.csv"
	iterations = 10 // Number of iterations per benchmark
)

func simulateWALIO() []byte {
	num := rand.Intn(100)
	uuid := gofakeit.UUID()

	data := make([]byte, 4+len(uuid))
	binary.BigEndian.PutUint32(data[:4], uint32(num))
	copy(data[4:], uuid)

	return data
}

// sync.Cond
func benchmarkSyncCond(numGoroutines int) time.Duration {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	var barrier sync.WaitGroup
	wg.Add(numGoroutines)
	ready.Add(numGoroutines)
	done := false

	start := time.Now()
	for j := 0; j < numGoroutines; j++ {
		go func() {
			mu.Lock()
			ready.Done()
			for !done {
				cond.Wait()
				_ = simulateWALIO()
				barrier.Done()
			}
			mu.Unlock()
			wg.Done()
		}()
	}
	_ = simulateWALIO()

	ready.Wait()
	for i := 0; i < iterations; i++ {
		barrier.Add(numGoroutines)
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
		barrier.Wait()
	}

	barrier.Add(numGoroutines)
	mu.Lock()
	done = true
	cond.Broadcast()
	mu.Unlock()

	return time.Since(start)
}

// Buffered Channel
func benchmarkBufferedChannel(numGoroutines int) time.Duration {
	ch := make(chan struct{}, 1)

	var wg sync.WaitGroup
	var ready sync.WaitGroup
	wg.Add(numGoroutines)
	ready.Add(numGoroutines)
	done := make(chan struct{})

	start := time.Now()
	for j := 0; j < numGoroutines; j++ {
		go func() {
			ready.Done()
			defer wg.Done()
			for {
				select {
				case <-ch:
					_ = simulateWALIO()
				case <-done:
					return
				}
			}

		}()
	}

	_ = simulateWALIO()
	ready.Wait()
	for i := 0; i < iterations; i++ {
		for j := 0; j < numGoroutines; j++ {
			ch <- struct{}{}
		}
	}

	close(done)
	wg.Wait()
	close(ch)
	return time.Since(start)
}

// Unbuffered Channel
func benchmarkUnbufferedChannel(numGoroutines int) time.Duration {
	ch := make(chan struct{})

	var wg sync.WaitGroup
	var ready sync.WaitGroup
	wg.Add(numGoroutines)
	ready.Add(numGoroutines)
	done := make(chan struct{})

	start := time.Now()
	for j := 0; j < numGoroutines; j++ {
		go func() {
			ready.Done()
			defer wg.Done()
			for {
				select {
				case <-ch:
					_ = simulateWALIO()
				case <-done:
					return
				}
			}

		}()
	}

	_ = simulateWALIO()
	ready.Wait()
	for i := 0; i < iterations; i++ {
		for j := 0; j < numGoroutines; j++ {
			ch <- struct{}{}
		}
	}

	close(done)
	wg.Wait()
	close(ch)
	return time.Since(start)
}

func writeResultsToCSV(writer *csv.Writer, results [][]string) {

	writer.Write([]string{"Method", "Avg Time Per Iteration (ms)", "goroutines"})
	for _, result := range results {
		writer.Write(result)
	}

	fmt.Println("Results written to", csvFile)
}

func main() {
	file, err := os.Create(csvFile)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, numGoroutines := range []int{100, 1000, 5000} {
		fmt.Println("Benchmarking with", numGoroutines, "goroutines", "for", iterations, "iteration")
		var totalSyncCond, totalBuffered, totalUnbuffered time.Duration
		for i := 0; i < iterations; i++ {
			totalSyncCond += benchmarkSyncCond(numGoroutines)
			totalBuffered += benchmarkBufferedChannel(numGoroutines)
			totalUnbuffered += benchmarkUnbufferedChannel(numGoroutines)
		}

		avgSyncCond := totalSyncCond.Seconds() * 1000 / float64(iterations)
		avgBuffered := totalBuffered.Seconds() * 1000 / float64(iterations)
		avgUnbuffered := totalUnbuffered.Seconds() * 1000 / float64(iterations)

		fmt.Printf("sync.Cond average time per iteration: %.6f ms\n", avgSyncCond)
		fmt.Printf("Buffered channel average time per iteration: %.6f ms\n", avgBuffered)
		fmt.Printf("Unbuffered channel average time per iteration: %.6f ms\n", avgUnbuffered)

		results := [][]string{
			{"sync.Cond", fmt.Sprintf("%.6f", avgSyncCond), strconv.Itoa(numGoroutines)},
			{"Buffered Channel", fmt.Sprintf("%.6f", avgBuffered), strconv.Itoa(numGoroutines)},
			{"Unbuffered Channel", fmt.Sprintf("%.6f", avgUnbuffered), strconv.Itoa(numGoroutines)},
		}

		writeResultsToCSV(writer, results)
	}

}
