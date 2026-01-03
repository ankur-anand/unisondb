package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/cliapp"
	"github.com/urfave/cli/v2"
)

func main() {
	cliapp.PrintBanner()
	var commands []*cli.Command

	if replicatorCommand != nil {
		commands = append(commands, replicatorCommand)
	}

	commands = append(commands, &cli.Command{
		Name:  "relayer",
		Usage: "Run in relayer mode",
		Action: func(c *cli.Context) error {
			return Run(c.Context, c.String("config"), c.String("env"),
				"relayer", c.Bool("grpc"), c.String("ports-file"))
		},
	})

	if fuzzerCommand != nil {
		commands = append(commands, fuzzerCommand)
	}

	app := &cli.App{
		Name:  "unisondb",
		Usage: "Run UnisonDB",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Value:   "./config.toml",
				Usage:   "Path to TOML config file",
				EnvVars: []string{"UNISON_CONFIG"},
			},
			&cli.StringFlag{
				Name:    "env",
				Aliases: []string{"e"},
				Value:   "dev",
				Usage:   "Environment: dev, staging, prod",
				EnvVars: []string{"UNISON_ENV"},
			},
			&cli.BoolFlag{
				Name:    "grpc",
				Aliases: []string{"G"},
				Usage:   "Enable gRPC server in Relayer Mode",
				EnvVars: []string{"UNISON_GRPC_ENABLED"},
				Value:   false,
			},
			&cli.StringFlag{
				Name:  "ports-file",
				Usage: "Write bound ports to this JSON file after startup (for testing)",
			},
		},

		Commands: commands,
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("[unisondb.main]",
			slog.String("event_type", "service.start.errored"),
			slog.Any("error", err),
		)
		os.Exit(1)
	}
}

// Run starts the UnisonDB server with the specified configuration.
func Run(_ context.Context, configPath, env, mode string, grpcEnabled bool, portsFile string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := cliapp.Server{PortsFile: portsFile}

	infraSetup := []func(context.Context) error{
		srv.InitFromCLI(configPath, env, mode, grpcEnabled),
		srv.InitTelemetry,
		srv.SetupStorageConfig,
		srv.SetupNotifier,
		srv.SetupStorage,
	}

	for i, fn := range infraSetup {
		if err := fn(ctx); err != nil {
			slog.Error("[unisondb.main]",
				slog.String("event_type", "infrastructure.setup.failed"),
				slog.Any("error", err),
				slog.Int("step", i),
			)
			return err
		}
	}

	srv.BuildDeps()

	srv.Register(&cliapp.GRPCService{})
	srv.Register(&cliapp.HTTPService{})
	srv.Register(&cliapp.RelayerService{})
	srv.Register(&cliapp.FuzzerService{})
	srv.Register(&cliapp.PProfService{})
	srv.Register(&cliapp.OffsetLoggerService{})
	srv.Register(&cliapp.StreamAgeService{})

	if err := srv.SetupServices(ctx); err != nil {
		slog.Error("[unisondb.main]",
			slog.String("event_type", "service.setup.failed"),
			slog.Any("error", err),
		)
		return err
	}

	slog.Info("[unisondb.main]",
		slog.String("event_type", "service.initialization.completed"),
		slog.Group("service",
			slog.String("mode", mode),
			slog.String("env", env),
			slog.String("config_path", configPath)),
	)

	err := srv.RunServices(ctx)
	shutdownReason := "context.cancelled.signal"
	if err != nil && !errors.Is(err, context.Canceled) {
		shutdownReason = "errored"
		slog.Error("[unisondb.main]", slog.String("event_type", "service.shutdown.started"),
			slog.String("shutdown_reason", shutdownReason), slog.Any("error", err))
	} else {
		slog.Info("[unisondb.main]", slog.String("event_type", "service.shutdown.started"),
			slog.String("shutdown_reason", shutdownReason))
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	srv.CloseServices(shutdownCtx)

	slices.Reverse(srv.DeferCallback)
	for i, cb := range srv.DeferCallback {
		slog.Debug("[unisondb.main] executing shutdown callback", "index", i)
		cb(shutdownCtx)
	}

	slog.Info("[unisondb.main]", slog.String("event_type", "service.shutdown.completed"),
		slog.String("mode", mode), slog.String("env", env))
	return err
}
