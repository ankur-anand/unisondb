package relayer

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalWalRelayer(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "relayer"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, engine.Close(context.Background()))
	})

	ctx, cancel := context.WithCancel(t.Context())
	writerDone := make(chan struct{})
	var writerErr error
	// Registered after engine cleanup so an in-flight write finishes before Close.
	t.Cleanup(func() {
		cancel()
		<-writerDone
		assert.NoError(t, writerErr)
	})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := engine.PutKV([]byte(gofakeit.UUID()), []byte(gofakeit.UUID()))
				if err != nil {
					writerErr = err
					return
				}

			case <-ctx.Done():
				return
			}
		}
	}()
	startTime := time.Now()
	hist, err := StartNLocalRelayer(t.Context(), engine, 10, 10*time.Millisecond)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	cancel()
	<-writerDone

	ReportReplicationStats(hist, engine.Namespace(), startTime)
}
