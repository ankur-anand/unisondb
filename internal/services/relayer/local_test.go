package relayer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
)

func TestLocalWalRelayer(t *testing.T) {
	baseDir := t.TempDir()
	namespace := "relayer"

	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, engine.Close(context.Background()))
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := engine.PutKV([]byte(gofakeit.UUID()), []byte(gofakeit.UUID()))
				if errors.Is(err, dbkernel.ErrInCloseProcess) {
					return
				}
				if err != nil {
					panic(err)
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

	ReportReplicationStats(hist, engine.Namespace(), startTime)
}
