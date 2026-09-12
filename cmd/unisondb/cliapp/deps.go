package cliapp

import (
	"log/slog"

	"github.com/ankur-anand/unisondb/cmd/unisondb/config"
	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/grpcutils"
	"github.com/ankur-anand/unisondb/internal/services/fuzzer"
	"github.com/ankur-anand/unisondb/pkg/notifier"
)

type Dependencies struct {
	Mode   string
	Env    string
	Config config.Config

	// Storage
	Engines       map[string]*dbkernel.Engine
	StorageConfig *dbkernel.EngineConfig
	Notifiers     map[string]notifier.Notifier

	// Telemetry
	Logger       *slog.Logger
	FuzzStats    *fuzzer.FuzzStats
	StatsHandler *grpcutils.GRPCStatsHandler
}
