package cliapp

import (
	"context"
	"testing"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuzzerService_Name(t *testing.T) {
	svc := &FuzzerService{}
	assert.Equal(t, "fuzzer", svc.Name())
}

func TestFuzzerService_Setup_DisabledInServerMode(t *testing.T) {
	deps := &Dependencies{
		Mode:    "server",
		Config:  config.Config{},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &FuzzerService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.False(t, svc.Enabled())
}

func TestFuzzerService_Setup_InvalidConfig(t *testing.T) {
	deps := &Dependencies{
		Mode: "fuzz",
		Config: config.Config{
			FuzzConfig: config.FuzzConfig{
				OpsPerNamespace:     0,
				WorkersPerNamespace: 0,
			},
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &FuzzerService{}
	err := svc.Setup(context.Background(), deps)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid fuzz config")
}

func TestFuzzerService_Setup_ValidConfig(t *testing.T) {
	deps := &Dependencies{
		Mode: "fuzz",
		Config: config.Config{
			FuzzConfig: config.FuzzConfig{
				OpsPerNamespace:     1000,
				WorkersPerNamespace: 4,
			},
		},
		Engines: map[string]*dbkernel.Engine{},
	}

	svc := &FuzzerService{}
	err := svc.Setup(context.Background(), deps)

	require.NoError(t, err)
	assert.True(t, svc.Enabled())
}

func TestFuzzerService_Run_NotEnabled(t *testing.T) {
	svc := &FuzzerService{enabled: false}
	err := svc.Run(context.Background())
	assert.NoError(t, err)
}

func TestFuzzerService_Close(t *testing.T) {
	svc := &FuzzerService{}
	err := svc.Close(context.Background())
	assert.NoError(t, err)
}
