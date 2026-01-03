package cliapp

import (
	"context"
	"testing"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetLoggerService_Name(t *testing.T) {
	svc := &OffsetLoggerService{}
	assert.Equal(t, "offset-logger", svc.Name())
}

func TestOffsetLoggerService_Setup(t *testing.T) {
	deps := &Dependencies{
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &OffsetLoggerService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.Equal(t, 1*time.Minute, svc.interval)
	assert.NotNil(t, svc.engines)
}

func TestOffsetLoggerService_Setup_CustomInterval(t *testing.T) {
	deps := &Dependencies{
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := NewOffsetLoggerService(5 * time.Second)
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, svc.interval)
}

func TestOffsetLoggerService_Run_ContextCancellation(t *testing.T) {
	svc := &OffsetLoggerService{
		engines:  map[string]*dbkernel.Engine{},
		interval: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)

	go func() {
		runDone <- svc.Run(ctx)
	}()

	time.Sleep(150 * time.Millisecond)

	cancel()

	select {
	case err := <-runDone:
		assert.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestOffsetLoggerService_Close(t *testing.T) {
	svc := &OffsetLoggerService{}
	err := svc.Close(context.Background())
	assert.NoError(t, err)
}

func TestStreamAgeService_Name(t *testing.T) {
	svc := &StreamAgeService{}
	assert.Equal(t, "stream-age", svc.Name())
}

func TestStreamAgeService_Setup(t *testing.T) {
	deps := &Dependencies{}

	svc := &StreamAgeService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, svc.interval)
}

func TestStreamAgeService_Setup_CustomInterval(t *testing.T) {
	deps := &Dependencies{}

	svc := NewStreamAgeService(10 * time.Second)
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, svc.interval)
}

func TestStreamAgeService_Run_NilStatsHandler(t *testing.T) {
	svc := &StreamAgeService{
		statsHandler: nil,
	}

	err := svc.Run(context.Background())
	assert.NoError(t, err)
}

func TestStreamAgeService_Close(t *testing.T) {
	svc := &StreamAgeService{}
	err := svc.Close(context.Background())
	assert.NoError(t, err)
}
