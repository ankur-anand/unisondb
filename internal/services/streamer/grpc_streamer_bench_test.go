package streamer_test

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/logcodec"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/brianvoe/gofakeit/v7"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const (
	benchListenerBuffSize = 1024 * 1024
)

func BenchmarkWALStreaming(b *testing.B) {
	scenarios := []struct {
		name        string
		recordCount int
		valueSize   int
		batchSize   int
		timeout     time.Duration
	}{
		{"SmallRecords_100", 100, 100, 20, 5 * time.Second},
		{"SmallRecords_1000", 1000, 100, 20, 10 * time.Second},
		{"MediumRecords_100", 100, 1024, 20, 5 * time.Second},
		{"MediumRecords_1000", 1000, 1024, 20, 10 * time.Second},
		{"LargeRecords_100", 100, 10240, 20, 5 * time.Second},
		{"MixedSizes_1000", 1000, -1, 20, 10 * time.Second},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkWALStream(b, scenario.recordCount, scenario.valueSize, scenario.batchSize, scenario.timeout)
		})
	}
}

func benchmarkWALStream(b *testing.B, recordCount, valueSize, batchSize int, timeout time.Duration) {
	b.Helper()

	nameSpace := strings.ToLower(gofakeit.Noun())
	dir := b.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		b.Fatal(err)
	}

	se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer se.Close(context.Background())

	for i := 0; i < recordCount; i++ {
		key := []byte(gofakeit.UUID())
		var value []byte
		if valueSize == -1 {
			size := 100
			if rand.Float64() < 0.2 {
				size = 1024
			}
			if rand.Float64() < 0.1 {
				size = 10240
			}
			value = bytes.Repeat([]byte("x"), size)
		} else {
			value = bytes.Repeat([]byte("x"), valueSize)
		}
		if err := se.PutKV(key, value); err != nil {
			b.Fatal(err)
		}
	}

	engines := map[string]*dbkernel.Engine{nameSpace: se}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGrp := &errgroup.Group{}
	server := streamer.NewGrpcStreamer(errGrp, engines, timeout)
	defer server.Close()

	listener := bufconn.Listen(benchListenerBuffSize)
	defer listener.Close()

	il := grpcutils.NewStatefulInterceptor(slog.New(slog.NewTextHandler(os.Stdout, nil)), make(map[string]bool))
	gS := grpc.NewServer(
		grpc.ChainStreamInterceptor(
			grpcutils.RequireNamespaceInterceptor,
			grpcutils.RequestIDStreamInterceptor,
			il.TelemetryStreamInterceptor,
		),
	)
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		_ = gS.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	client := v1.NewWalStreamerServiceClient(conn)

	b.ResetTimer()
	b.ReportAllocs()

	var totalRecords int64
	var totalBytes int64

	for i := 0; i < b.N; i++ {
		md := metadata.Pairs("x-namespace", nameSpace)
		streamCtx, streamCancel := context.WithCancel(metadata.NewOutgoingContext(ctx, md))
		defer streamCancel()

		stream, err := client.StreamWalRecords(streamCtx, &v1.StreamWalRecordsRequest{})
		if err != nil {
			b.Fatal(err)
		}

		receivedRecords := 0
		receivedBytes := 0

		for {
			resp, err := stream.Recv()
			if err != nil {
				break
			}
			receivedRecords += len(resp.Records)
			for _, rec := range resp.Records {
				receivedBytes += len(rec.Record)
			}

			if receivedRecords >= recordCount {
				streamCancel()
			}
		}

		totalRecords += int64(receivedRecords)
		totalBytes += int64(receivedBytes)
	}

	b.StopTimer()

	avgBytesPerRun := float64(totalBytes) / float64(b.N)
	b.ReportMetric(avgBytesPerRun/1024/1024, "MB/op")

	if b.Elapsed() > 0 {
		recordsPerSec := float64(totalRecords) / b.Elapsed().Seconds()
		b.ReportMetric(recordsPerSec, "rec/s")
	}
}

func BenchmarkContinuousWriteAndStream(b *testing.B) {
	scenarios := []struct {
		name      string
		writeRPS  int // Target writes per second
		duration  time.Duration
		valueSize int
	}{
		{"LowRate_100RPS_SmallValues", 100, 5 * time.Second, 100},
		{"MediumRate_500RPS_SmallValues", 500, 5 * time.Second, 100},
		{"HighRate_1000RPS_SmallValues", 1000, 5 * time.Second, 100},
		{"MediumRate_500RPS_LargeValues", 500, 5 * time.Second, 1024},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkContinuousWriteAndStream(b, scenario.writeRPS, scenario.duration, scenario.valueSize)
		})
	}
}

func benchmarkContinuousWriteAndStream(b *testing.B, writeRPS int, duration time.Duration, valueSize int) {
	b.Helper()

	// Setup engine
	nameSpace := strings.ToLower(gofakeit.Noun())
	dir := b.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		b.Fatal(err)
	}

	se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer se.Close(context.Background())

	engines := map[string]*dbkernel.Engine{nameSpace: se}

	// Setup gRPC server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGrp := &errgroup.Group{}
	server := streamer.NewGrpcStreamer(errGrp, engines, 10*time.Second)
	defer server.Close()

	listener := bufconn.Listen(benchListenerBuffSize)
	defer listener.Close()

	il := grpcutils.NewStatefulInterceptor(slog.New(slog.NewTextHandler(os.Stdout, nil)), make(map[string]bool))
	gS := grpc.NewServer(
		grpc.ChainStreamInterceptor(
			grpcutils.RequireNamespaceInterceptor,
			grpcutils.RequestIDStreamInterceptor,
			il.TelemetryStreamInterceptor,
		),
	)
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		_ = gS.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	client := v1.NewWalStreamerServiceClient(conn)

	type writeRecord struct {
		key       []byte
		timestamp time.Time
	}
	writeChan := make(chan writeRecord, writeRPS*2)

	md := metadata.Pairs("x-namespace", nameSpace)
	streamCtx, streamCancel := context.WithCancel(metadata.NewOutgoingContext(ctx, md))
	defer streamCancel()

	stream, err := client.StreamWalRecords(streamCtx, &v1.StreamWalRecordsRequest{})
	if err != nil {
		b.Fatal(err)
	}

	var (
		latencies     []time.Duration
		latenciesMu   sync.Mutex
		recordsRead   atomic.Int64
		recordsFailed atomic.Int64
	)

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		writeMap := make(map[string]time.Time, writeRPS*10)
		var writeMapMu sync.Mutex

		go func() {
			for wr := range writeChan {
				writeMapMu.Lock()
				writeMap[string(wr.key)] = wr.timestamp
				writeMapMu.Unlock()
			}
		}()

		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}

			receiveTime := time.Now()
			for _, walRecord := range resp.Records {

				fbRecord := logrecord.GetRootAsLogRecord(walRecord.Record, 0)
				logEntry := logcodec.DeserializeFBRootLogRecord(fbRecord)

				for _, entry := range logEntry.Entries {
					kvEntry := logcodec.DeserializeKVEntry(entry)
					key := kvEntry.Key
					writeMapMu.Lock()
					if writeTime, exists := writeMap[string(key)]; exists {
						latency := receiveTime.Sub(writeTime)
						latenciesMu.Lock()
						latencies = append(latencies, latency)
						latenciesMu.Unlock()
						recordsRead.Add(1)
						delete(writeMap, string(key))
					}
					writeMapMu.Unlock()
				}
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	writeInterval := time.Second / time.Duration(writeRPS)
	ticker := time.NewTicker(writeInterval)
	defer ticker.Stop()

	timeout := time.After(duration)
	writesCompleted := 0

writeLoop:
	for {
		select {
		case <-timeout:
			break writeLoop
		case <-ticker.C:
			key := []byte(gofakeit.UUID())
			value := bytes.Repeat([]byte("x"), valueSize)
			writeTime := time.Now()

			if err := se.PutKV(key, value); err != nil {
				recordsFailed.Add(1)
				continue
			}

			select {
			case writeChan <- writeRecord{key: key, timestamp: writeTime}:
			default:

			}
			writesCompleted++
		}
	}

	time.Sleep(500 * time.Millisecond)
	streamCancel()
	close(writeChan)

	select {
	case <-streamDone:
	case <-time.After(2 * time.Second):
	}

	b.StopTimer()

	latenciesMu.Lock()
	defer latenciesMu.Unlock()

	if len(latencies) == 0 {
		b.Fatal("No latencies recorded")
	}

	sortedLatencies := make([]time.Duration, len(latencies))
	copy(sortedLatencies, latencies)
	sort.Slice(sortedLatencies, func(i, j int) bool {
		return sortedLatencies[i] < sortedLatencies[j]
	})

	p50 := sortedLatencies[len(sortedLatencies)*50/100]
	p99 := sortedLatencies[len(sortedLatencies)*99/100]

	actualRPS := float64(writesCompleted) / duration.Seconds()
	successRate := float64(recordsRead.Load()) / float64(writesCompleted) * 100

	b.ReportMetric(p50.Seconds()*1000, "p50_ms")
	b.ReportMetric(p99.Seconds()*1000, "p99_ms")
	b.ReportMetric(actualRPS, "rps")
	b.ReportMetric(successRate, "%")
}

func BenchmarkMultiClientStreaming(b *testing.B) {
	scenarios := []struct {
		name       string
		numClients int
		writeRPS   int
		duration   time.Duration
		valueSize  int
	}{
		{"10Clients_100RPS", 10, 100, 10 * time.Second, 100},
		{"50Clients_500RPS", 50, 500, 10 * time.Second, 100},
		{"100Clients_1000RPS", 100, 1000, 10 * time.Second, 100},
		{"200Clients_1000RPS", 200, 1000, 10 * time.Second, 100},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkMultiClientStreaming(b, scenario.numClients, scenario.writeRPS, scenario.duration, scenario.valueSize)
		})
	}
}

func benchmarkMultiClientStreaming(b *testing.B, numClients, writeRPS int, duration time.Duration, valueSize int) {
	b.Helper()

	nameSpace := strings.ToLower(gofakeit.Noun())
	dir := b.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		b.Fatal(err)
	}

	se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer se.Close(context.Background())

	engines := map[string]*dbkernel.Engine{nameSpace: se}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGrp := &errgroup.Group{}
	server := streamer.NewGrpcStreamer(errGrp, engines, 30*time.Second)
	defer server.Close()

	listener := bufconn.Listen(benchListenerBuffSize)
	defer listener.Close()

	il := grpcutils.NewStatefulInterceptor(slog.New(slog.NewTextHandler(os.Stdout, nil)), make(map[string]bool))
	gS := grpc.NewServer(
		grpc.ChainStreamInterceptor(
			grpcutils.RequireNamespaceInterceptor,
			grpcutils.RequestIDStreamInterceptor,
			il.TelemetryStreamInterceptor,
		),
	)
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		_ = gS.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	grpcClient := v1.NewWalStreamerServiceClient(conn)

	type writeRecord struct {
		key       string
		timestamp time.Time
	}
	writeChan := make(chan writeRecord, writeRPS*2)

	type clientStats struct {
		clientID        int
		recordsReceived atomic.Int64
		latencies       []time.Duration
		latenciesMu     sync.Mutex
	}

	clientStatsSlice := make([]*clientStats, numClients)
	var clientsReady atomic.Int32
	var clientsDone sync.WaitGroup

	for i := 0; i < numClients; i++ {
		clientStatsSlice[i] = &clientStats{
			clientID:  i,
			latencies: make([]time.Duration, 0, writeRPS/numClients+100),
		}

		clientsDone.Add(1)
		go func(stats *clientStats) {
			defer clientsDone.Done()

			md := metadata.Pairs("x-namespace", nameSpace)
			streamCtx, streamCancel := context.WithCancel(metadata.NewOutgoingContext(ctx, md))
			defer streamCancel()

			stream, err := grpcClient.StreamWalRecords(streamCtx, &v1.StreamWalRecordsRequest{})
			if err != nil {
				b.Errorf("Client %d failed to start stream: %v", stats.clientID, err)
				return
			}

			writeMap := make(map[string]time.Time, writeRPS*2)
			var writeMapMu sync.Mutex

			localWriteChan := make(chan writeRecord, 100)
			go func() {
				for wr := range writeChan {
					writeMapMu.Lock()
					writeMap[wr.key] = wr.timestamp
					writeMapMu.Unlock()
					select {
					case localWriteChan <- wr:
					default:
					}
				}
				close(localWriteChan)
			}()

			clientsReady.Add(1)

			for {
				resp, err := stream.Recv()
				if err != nil {
					return
				}

				receiveTime := time.Now()
				for _, walRecord := range resp.Records {
					fbRecord := logrecord.GetRootAsLogRecord(walRecord.Record, 0)
					logEntry := logcodec.DeserializeFBRootLogRecord(fbRecord)

					for _, entry := range logEntry.Entries {
						kvEntry := logcodec.DeserializeKVEntry(entry)
						key := string(kvEntry.Key)
						writeMapMu.Lock()
						if writeTime, exists := writeMap[key]; exists {
							latency := receiveTime.Sub(writeTime)
							stats.latenciesMu.Lock()
							stats.latencies = append(stats.latencies, latency)
							stats.latenciesMu.Unlock()
							stats.recordsReceived.Add(1)
							delete(writeMap, key)
						}
						writeMapMu.Unlock()
					}
				}
			}
		}(clientStatsSlice[i])
	}

	for clientsReady.Load() < int32(numClients) {
		time.Sleep(10 * time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()

	writeInterval := time.Second / time.Duration(writeRPS)
	ticker := time.NewTicker(writeInterval)
	defer ticker.Stop()

	timeout := time.After(duration)
	writesCompleted := 0

writeLoop:
	for {
		select {
		case <-timeout:
			break writeLoop
		case <-ticker.C:
			key := gofakeit.UUID()
			value := bytes.Repeat([]byte("x"), valueSize)
			writeTime := time.Now()

			if err := se.PutKV([]byte(key), value); err != nil {
				continue
			}

			select {
			case writeChan <- writeRecord{key: key, timestamp: writeTime}:
			default:

			}
			writesCompleted++
		}
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	close(writeChan)

	clientsDone.Wait()

	b.StopTimer()

	var (
		totalRecordsReceived int64
		allLatencies         []time.Duration
	)

	for _, stats := range clientStatsSlice {
		totalRecordsReceived += stats.recordsReceived.Load()
		stats.latenciesMu.Lock()
		allLatencies = append(allLatencies, stats.latencies...)
		stats.latenciesMu.Unlock()
	}

	if len(allLatencies) == 0 {
		b.Fatal("No latencies recorded across any client")
	}

	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	p50 := allLatencies[len(allLatencies)*50/100]
	p99 := allLatencies[len(allLatencies)*99/100]

	actualRPS := float64(writesCompleted) / duration.Seconds()
	successRate := float64(totalRecordsReceived) / float64(writesCompleted) * 100

	b.ReportMetric(p50.Seconds()*1000, "p50_ms")
	b.ReportMetric(p99.Seconds()*1000, "p99_ms")
	b.ReportMetric(actualRPS, "rps")
	b.ReportMetric(successRate, "%")
	b.ReportMetric(float64(numClients), "clients")
}
