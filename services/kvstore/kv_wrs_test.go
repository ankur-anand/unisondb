package kvstore_test

import (
	"bytes"
	"context"
	"hash/crc32"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/ankur-anand/kvalchemy/internal/middleware"
	v1 "github.com/ankur-anand/kvalchemy/proto/gen/go/kvalchemy/replicator/v1"
	"github.com/ankur-anand/kvalchemy/services/kvstore"
	"github.com/ankur-anand/kvalchemy/splitter"
	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type readWriterService struct {
	reader *kvstore.KVReaderService
	writer *kvstore.KVWriterService
}

const (
	listenerBuffSize = 1024
	chunkSizeMB      = 1 * 1024 * 1024
)

func bufDialer(lis *bufconn.Listener) func(ctx context.Context, s string) (net.Conn, error) {
	return func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}
}

// GenerateTestData generates a large dataset for chunking.
func GenerateTestData(size int) []byte {
	value := []byte(gofakeit.LetterN(20))
	data := bytes.Repeat(value, size/(len(value))+1)
	return data[:size]
}

func TestClient_PutKV_GetKV_DeleteKV(t *testing.T) {
	var engines = make(map[string]*storage.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close()
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "kvalchemy")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := storage.NewStorageEngine(temp, nameSpace, nil)
		if err != nil {
			panic(err)
		}
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := kvstore.NewKVReaderService(engines)
	writer := kvstore.NewKVWriterService(engines)

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()

	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.MethodInterceptor,
		middleware.TelemetryInterceptor),
		grpc.ChainUnaryInterceptor(middleware.RequestIDUnaryInterceptor,
			middleware.CorrelationIDUnaryInterceptor,
			middleware.MethodUnaryInterceptor,
			middleware.TelemetryUnaryInterceptor,
		))
	defer gS.Stop()

	go func() {
		v1.RegisterKVStoreReadServiceServer(gS, reader)
		v1.RegisterKVStoreWriteServiceServer(gS, writer)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")
	client := kvstore.NewClient(conn)

	keyValue := make(map[string]string)
	t.Run("put", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := gofakeit.Noun()
			value := gofakeit.Sentence(20)
			keyValue[key] = value
			err := client.PutKV(ctx, nameSpaces[0], key, []byte(value))
			assert.NoError(t, err, "failed to put kv")
		}
	})

	t.Run("get", func(t *testing.T) {
		for k, v := range keyValue {
			value, err := client.GetKV(ctx, nameSpaces[0], k)
			assert.NoError(t, err, "failed to get kv")
			assert.Equal(t, v, string(value), "value mismatch")
		}

	})

	t.Run("delete", func(t *testing.T) {
		for k := range keyValue {
			assert.NoError(t, client.DeleteKV(ctx, nameSpaces[0], k), "failed to delete kv")
		}

	})

	t.Run("get_after_get", func(t *testing.T) {
		for k := range keyValue {
			_, err := client.GetKV(ctx, nameSpaces[0], k)
			assert.ErrorIs(t, err, kvstore.ErrKeyNotFound, "failed to get kv")
		}
	})

	t.Run("value_greater_than_1MB", func(t *testing.T) {
		data := GenerateTestData(5 * chunkSizeMB)
		err = client.PutKV(ctx, "random", "key", data)
		assert.ErrorIs(t, err, kvstore.ErrValueSizeLimitExceeded, "failed to put kv")
	})

}

func TestClient_PutStreamChunksForKey(t *testing.T) {
	var engines = make(map[string]*storage.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close()
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "kvalchemy")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := storage.NewStorageEngine(temp, nameSpace, nil)
		if err != nil {
			panic(err)
		}
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := kvstore.NewKVReaderService(engines)
	writer := kvstore.NewKVWriterService(engines)

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()

	gS := grpc.NewServer(grpc.ChainStreamInterceptor(middleware.RequireNamespaceInterceptor,
		middleware.RequestIDStreamInterceptor,
		middleware.CorrelationIDStreamInterceptor,
		middleware.MethodInterceptor,
		middleware.TelemetryInterceptor),
		grpc.ChainUnaryInterceptor(middleware.RequestIDUnaryInterceptor,
			middleware.CorrelationIDUnaryInterceptor,
			middleware.MethodUnaryInterceptor,
			middleware.TelemetryUnaryInterceptor,
		))
	defer gS.Stop()

	go func() {
		v1.RegisterKVStoreReadServiceServer(gS, reader)
		v1.RegisterKVStoreWriteServiceServer(gS, writer)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")
	client := kvstore.NewClient(conn)

	keyValue := make([][]byte, 0, 10)
	assembledValue := make([]byte, 0)
	checksum := uint32(0)
	for i := 0; i < 10; i++ {
		value := gofakeit.Sentence(2)
		keyValue = append(keyValue, []byte(value))
		assembledValue = append(assembledValue, value...)
		checksum = crc32.Update(checksum, crc32.IEEETable, []byte(value))
	}

	key := "chunk_random"
	assert.NoError(t, client.PutStreamChunksForKey(ctx, nameSpaces[0], key, keyValue), "failed to put chunks")

	value, err := client.GetKV(ctx, nameSpaces[0], key)
	assert.NoError(t, err)
	assert.Equal(t, assembledValue, value, "value mismatch")
	assert.Equal(t, checksum, splitter.ComputeChecksum(value), "checksum mismatch")

}
