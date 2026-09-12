package cliapp

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/objlog"
	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/testutil/objlogtest"
	"github.com/stretchr/testify/require"
)

func TestBlobStoreStreamerService_StandaloneStreamsAndResumes(t *testing.T) {
	engineDir := t.TempDir()
	namespace := "orders"
	engine, err := dbkernel.NewStorageEngine(engineDir, namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close(context.Background()))
	})

	for i := 0; i < 8; i++ {
		err = engine.PutKV([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i)))
		require.NoError(t, err)
	}
	initialLSN := engine.OpsReceivedCount()

	svc := &BlobStoreStreamerService{
		logs: map[string]*objlog.Log{namespace: objlogtest.NewLog(t, nil)},
	}
	deps := &Dependencies{
		Mode: "server",
		Config: config.Config{
			BlobStoreStreaming: config.BlobStoreStreamingConfig{
				Enabled:       true,
				FlushInterval: "50ms",
				Namespaces: map[string]config.BlobStoreWriteNSConfig{
					namespace: {
						BasePrefix: "unisondb",
					},
				},
			},
		},
		Engines: map[string]*dbkernel.Engine{
			namespace: engine,
		},
	}

	require.NoError(t, svc.Setup(context.Background(), deps))
	t.Cleanup(func() {
		require.NoError(t, svc.Close(context.Background()))
	})

	runOnce := func(ctx context.Context) error {
		var (
			runErr error
			wg     sync.WaitGroup
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runErr = svc.Run(ctx)
		}()

		<-ctx.Done()
		wg.Wait()
		return runErr
	}

	runCtx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() {
		done1 <- runOnce(runCtx1)
	}()

	require.Eventually(t, func() bool {
		lsn, err := svc.lastWrittenLSN(context.Background(), namespace)
		return err == nil && lsn == initialLSN
	}, 5*time.Second, 50*time.Millisecond)

	cancel1()
	require.NoError(t, <-done1)

	for i := 8; i < 12; i++ {
		err = engine.PutKV([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("value-%02d", i)))
		require.NoError(t, err)
	}
	finalLSN := engine.OpsReceivedCount()

	lsnBeforeRestart, err := svc.lastWrittenLSN(context.Background(), namespace)
	require.NoError(t, err)
	require.Equal(t, initialLSN, lsnBeforeRestart)

	runCtx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() {
		done2 <- runOnce(runCtx2)
	}()

	require.Eventually(t, func() bool {
		lsn, err := svc.lastWrittenLSN(context.Background(), namespace)
		return err == nil && lsn == finalLSN
	}, 5*time.Second, 50*time.Millisecond)

	cancel2()
	require.NoError(t, <-done2)
}
