package streamer_test

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/middleware"
	"github.com/ankur-anand/unisondb/internal/services"
	"github.com/ankur-anand/unisondb/internal/services/streamer"
	"github.com/ankur-anand/unisondb/schemas/logrecord"
	v1 "github.com/ankur-anand/unisondb/schemas/proto/gen/go/unisondb/streamer/v1"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"google.golang.org/grpc/test/bufconn"
)

const (
	listenerBuffSize = 1024
)

const (
	smallValue       = 100  // 100 bytes
	largeValue       = 1024 // 1024 bytes values
	largeValueChance = 0.2  // 20% chance of having large values
)

type noopWalIO struct {
	recordWalCount int
}

func (n *noopWalIO) Write(data *v1.WALRecord) error {
	n.recordWalCount++
	return nil
}

func bufDialer(lis *bufconn.Listener) func(ctx context.Context, s string) (net.Conn, error) {
	return func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}
}

func createEngine(t *testing.T) *dbkernel.Engine {
	t.Helper()
	nameSpace := gofakeit.Noun()

	dir := t.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		panic(err)
	}
	se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		_ = se.Close(context.Background())
	})
	return se
}

func Test_Invalid_Request_At_Server(t *testing.T) {
	engine := createEngine(t)
	engines := map[string]*dbkernel.Engine{
		engine.Namespace(): engine,
	}

	nameSpace := engine.Namespace()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGrp := &errgroup.Group{}
	server := streamer.NewGrpcStreamer(errGrp, engines, 1*time.Minute)

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.TelemetryInterceptor))
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {

			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")

	client := v1.NewWalStreamerServiceClient(conn)

	t.Run("MissingNamespace", func(t *testing.T) {
		offset, err := client.GetLatestOffset(ctx, &v1.GetLatestOffsetRequest{})
		assert.Error(t, err, "expected error on missing namespace")
		assert.Nil(t, offset, "expected error on missing namespace")

		wal, err := client.StreamWalRecords(ctx, &v1.StreamWalRecordsRequest{})
		assert.NoError(t, err, "failed to stream WAL")
		_, err = wal.Recv()
		assert.Error(t, err, "expected error on missing namespace")

		statusErr := status.Convert(err)
		assert.Equal(t, codes.InvalidArgument, statusErr.Code(), "expected error code InvalidArgument")
		assert.Equal(t, services.ErrMissingNamespaceInMetadata.Error(), statusErr.Message(), "expected error message didn't match")
	})

	// Case 2: Namespace does not exist
	t.Run("NamespaceNotExists", func(t *testing.T) {
		md := metadata.Pairs("x-namespace", "unknown")
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		wal, err := client.StreamWalRecords(ctx, &v1.StreamWalRecordsRequest{})
		assert.NoError(t, err, "failed to stream WAL")
		_, err = wal.Recv()
		assert.Error(t, err, "expected error on non-existent namespace")

		statusErr := status.Convert(err)
		assert.Equal(t, codes.NotFound, statusErr.Code(), "expected error code NotFound")
		assert.Equal(t, services.ErrNamespaceNotExists.Error(), statusErr.Message(), "expected error message didn't match")
	})

	// Case 3: Invalid Offset
	t.Run("InvalidMetadata", func(t *testing.T) {
		md := metadata.Pairs("x-namespace", nameSpace)
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		wal, err := client.StreamWalRecords(ctx, &v1.StreamWalRecordsRequest{
			Offset: []byte{255, 2, 3}, // Corrupt data
		})
		assert.NoError(t, err, "failed to stream WAL")
		_, err = wal.Recv()
		assert.Error(t, err, "expected error on invalid metadata")

		statusErr := status.Convert(err)
		assert.Equal(t, codes.InvalidArgument, statusErr.Code(),
			"expected error code InvalidArgument")
		assert.Equal(t, services.ErrInvalidMetadata.Error(),
			statusErr.Message(), "expected error message didn't match")
	})

	assert.NoError(t, conn.Close(), "failed to close grpc client")
}

func Test_Invalid_Request_At_Client(t *testing.T) {
	engine := createEngine(t)
	engines := map[string]*dbkernel.Engine{
		engine.Namespace(): engine,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGrp := &errgroup.Group{}
	server := streamer.NewGrpcStreamer(errGrp, engines, 1*time.Minute)

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.TelemetryInterceptor))
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {

			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")
	client := streamer.NewGrpcStreamerClient(conn, "nameSpaces.doesn't.exit", &noopWalIO{}, nil)

	resp, err := client.GetLatestOffset(ctx)
	assert.Error(t, err, "expected error on missing namespace")
	assert.Nil(t, resp, "expected error on missing namespace")

	err = client.StreamWAL(ctx)
	assert.Error(t, err, "expected error on missing namespace")

	err = conn.Close()
	assert.NoError(t, err, "failed to close grpc client")
}

func TestServer_StreamWAL_StreamTimeoutErr(t *testing.T) {
	var engines = make(map[string]*dbkernel.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close(context.Background())
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
		assert.NoError(t, err)
		assert.NotNil(t, se)
		// for each engine write the records, few of them being more than 1 MB in size.
		// write a total of
		for i := 0; i < 10; i++ {
			key := []byte(gofakeit.Noun())
			valueSize := smallValue
			if rand.Float64() < largeValueChance {
				valueSize = largeValue
			}
			value := []byte(gofakeit.LetterN(5))
			data := bytes.Repeat(value, valueSize)
			err = se.Put(key, data)
			assert.NoError(t, err)
		}
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGroup, _ := errgroup.WithContext(ctx)
	server := streamer.NewGrpcStreamer(errGroup, engines, 200*time.Millisecond)

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.TelemetryInterceptor))
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")
	client := v1.NewWalStreamerServiceClient(conn)

	errGroup.Go(func() error {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-ticker.C:
				key := []byte(gofakeit.Noun())
				valueSize := smallValue
				if rand.Float64() < largeValueChance {
					valueSize = largeValue
				}
				value := []byte(gofakeit.LetterN(1))
				data := bytes.Repeat(value, valueSize)
				err = engines[nameSpaces[0]].Put(key, data)
				assert.NoError(t, err)
			case <-timer.C:
				return nil
			}
		}
	})

	t.Run("wal-replication", func(t *testing.T) {
		md := metadata.Pairs("x-namespace", nameSpaces[0])
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		wal, err := client.StreamWalRecords(ctx, &v1.StreamWalRecordsRequest{})
		assert.NoError(t, err, "failed to stream WAL")

		valuesCount := 0
		lastRecvIndex := uint64(0)
		for {
			val, err := wal.Recv()
			if err != nil {
				statusErr := status.Convert(err)
				assert.Equal(t, codes.Unavailable, statusErr.Code(), "expected error code NotFound")
				assert.Equal(t, statusErr.Message(), services.ErrStreamTimeout.Error(), "expected error message didn't match")
				break
			}
			valuesCount = +len(val.Records)
			for _, record := range val.Records {
				lastRecvIndex++
				wr := logrecord.GetRootAsLogRecord(record.Record, 0)
				assert.NotNil(t, wr, "error converting to wal record")
				assert.Equal(t, lastRecvIndex, wr.Lsn(), "last recv index does not match")
			}

		}

		assert.Greater(t, valuesCount, 0, "total 20 logs should have streamed")
	})

	t.Run("offset_should_not_be_empty", func(t *testing.T) {
		md := metadata.Pairs("x-namespace", nameSpaces[0])
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		response, err := client.GetLatestOffset(ctx, &v1.GetLatestOffsetRequest{})
		assert.NoError(t, err)
		assert.NotNil(t, response.Offset)
	})

	errN := errGroup.Wait()
	assert.NoError(t, errN)
	assert.NoError(t, conn.Close())
}

func TestServer_StreamWAL_Client(t *testing.T) {
	var engines = make(map[string]*dbkernel.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close(context.Background())
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
		assert.NoError(t, err)
		assert.NotNil(t, se)
		// for each engine write the records, few of them being more than 1 MB in size.
		// write a total of
		for i := 0; i < 10; i++ {
			key := []byte(gofakeit.Noun())
			valueSize := smallValue
			if rand.Float64() < largeValueChance {
				valueSize = largeValue
			}
			value := []byte(gofakeit.LetterN(5))
			data := bytes.Repeat(value, valueSize)
			err = se.Put(key, data)
			assert.NoError(t, err)
		}
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGroup, _ := errgroup.WithContext(ctx)
	server := streamer.NewGrpcStreamer(errGroup, engines, 2*time.Second)

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.TelemetryInterceptor))
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")

	t.Run("get_offset", func(t *testing.T) {
		nw := &noopWalIO{}
		client := streamer.NewGrpcStreamerClient(conn, nameSpaces[0], nw, nil)
		resp, err := client.GetLatestOffset(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
	})

	t.Run("client_retry", func(t *testing.T) {
		nw := &noopWalIO{}
		client := streamer.NewGrpcStreamerClient(conn, nameSpaces[0], nw, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			time.Sleep(1 * time.Second)
			for i := 0; i < 10; i++ {
				key := []byte(gofakeit.Noun())
				valueSize := smallValue
				if rand.Float64() < largeValueChance {
					valueSize = largeValue
				}
				value := []byte(gofakeit.LetterN(5))
				data := bytes.Repeat(value, valueSize)
				err := engines[nameSpaces[0]].Put(key, data)
				assert.NoError(t, err)
			}
			cancel()
		}()
		err := client.StreamWAL(ctx)
		statusErr := status.Convert(err)
		assert.Equal(t, statusErr.Message(), context.Canceled.Error())
	})
	assert.NoError(t, conn.Close())
}

func TestServer_StreamWAL_MaxRetry(t *testing.T) {
	var engines = make(map[string]*dbkernel.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close(context.Background())
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
		assert.NoError(t, err)
		assert.NotNil(t, se)
		// for each engine write the records, few of them being more than 1 MB in size.
		// write a total of
		for i := 0; i < 10; i++ {
			key := []byte(gofakeit.Noun())
			valueSize := smallValue
			if rand.Float64() < largeValueChance {
				valueSize = largeValue
			}
			value := []byte(gofakeit.LetterN(5))
			data := bytes.Repeat(value, valueSize)
			err = se.Put(key, data)
			assert.NoError(t, err)
		}
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGroup, ctx := errgroup.WithContext(ctx)
	server := streamer.NewGrpcStreamer(errGroup, engines, 50*time.Millisecond)

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.TelemetryInterceptor))
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")
	t.Run("client_max_retry", func(t *testing.T) {
		client := streamer.NewGrpcStreamerClient(conn, nameSpaces[0], &noopWalIO{}, nil)
		err = client.StreamWAL(ctx)
		assert.ErrorIs(t, err, services.ErrClientMaxRetriesExceeded, "expected Error didn't happen")
	})
	assert.NoError(t, conn.Close())
}

func TestClientServer_StreamCancel(t *testing.T) {
	var engines = make(map[string]*dbkernel.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close(context.Background())
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "unisondb")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := dbkernel.NewStorageEngine(temp, nameSpace, dbkernel.NewDefaultEngineConfig())
		assert.NoError(t, err)
		assert.NotNil(t, se)
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGroup, _ := errgroup.WithContext(ctx)
	server := streamer.NewGrpcStreamer(errGroup, engines, 200*time.Millisecond)

	listener := bufconn.Listen(listenerBuffSize)
	sCodeIn := &streamErrorCodeInjector{
		code: codes.ResourceExhausted,
	}

	canceller := streamCanceller{}
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.TelemetryInterceptor, canceller.CancelInterceptor,
	),
		grpc.InTapHandle(sCodeIn.MyServerInHandle))
	defer gS.Stop()

	go func() {
		v1.RegisterWalStreamerServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")

	t.Run("empty_offset", func(t *testing.T) {
		client := streamer.NewGrpcStreamerClient(conn, nameSpaces[0], &noopWalIO{}, nil)
		off, err := client.GetLatestOffset(ctx)
		assert.NoError(t, err)
		assert.Nil(t, off)
	})

	t.Run("inject_stream_fail", func(t *testing.T) {
		//sCodeIn.once.Store(true)
		go func() {
			time.Sleep(5 * time.Second)
			canceller.Lock()
			canceller.cancel()
			canceller.Unlock()
		}()
		client := streamer.NewGrpcStreamerClient(conn, nameSpaces[0], &noopWalIO{}, nil)
		err := client.StreamWAL(ctx)
		assert.Error(t, err)
	})

	errN := errGroup.Wait()
	assert.NoError(t, errN)
	assert.NoError(t, conn.Close())
}

type streamErrorCodeInjector struct {
	once atomic.Bool
	code codes.Code
}

// StreamErrorCodeInjectorInterceptor injects errors based on some logic
func (s *streamErrorCodeInjector) MyServerInHandle(ctx context.Context, info *tap.Info) (context.Context, error) {
	if s.once.Load() {
		return ctx, status.Error(s.code, "injected: server side error")
	}
	// Otherwise proceed normally
	return ctx, nil
}

type streamCanceller struct {
	sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *streamCanceller) CancelInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Replace original context with our wrapped one
	ctx, cancel := context.WithCancel(ss.Context())
	s.Lock()
	s.cancel = cancel
	s.ctx = ctx
	s.Unlock()
	wrapped := &wrappedServerStream{
		ServerStream: ss,
		ctx:          ctx,
	}

	return handler(srv, wrapped)
}

// wrappedServerStream wraps gRPC ServerStream to modify the context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
