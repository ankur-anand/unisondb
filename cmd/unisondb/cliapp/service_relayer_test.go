package cliapp

import (
	"context"
	"testing"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelayerService_Name(t *testing.T) {
	svc := &RelayerService{}
	assert.Equal(t, "relayer", svc.Name())
}

func TestRelayerService_Setup_DisabledInServerMode(t *testing.T) {
	deps := &Dependencies{
		Mode:    "server",
		Config:  config.Config{},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &RelayerService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.False(t, svc.Enabled())
}

func TestRelayerService_Run_NotEnabled(t *testing.T) {
	svc := &RelayerService{enabled: false}
	err := svc.Run(context.Background())
	assert.NoError(t, err)
}

func TestRelayerService_Close_NoConnections(t *testing.T) {
	svc := &RelayerService{}
	err := svc.Close(context.Background())
	assert.NoError(t, err)
}
