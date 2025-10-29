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
	"golang.org/x/sync/errgroup"
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
				"relayer", c.Bool("grpc"))
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

// Run
// nolint: funlen
func Run(_ context.Context, configPath, env, mode string, grpcEnabled bool) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := cliapp.Server{}

	setup := []func(context.Context) error{
		srv.InitFromCLI(configPath, env, mode, grpcEnabled),
		srv.InitTelemetry,
		srv.SetupStorageConfig,
		srv.SetupNotifier,
		srv.SetupStorage,
		srv.SetupGrpcServer,
		srv.SetupHTTPServer,
		srv.SetupRelayer,
		srv.SetupPprofServer,
	}

	for i, fn := range setup {
		if err := fn(ctx); err != nil {
			slog.Error("[unisondb.main]",
				slog.String("event_type", "service.initialization.failed"),
				slog.Any("error", err),
				slog.Int("step", i),
			)
			return err
		}
	}

	//IMP: run all the services concurrently
	runFns := []func(context.Context) error{
		srv.RunGrpc,
		srv.RunHTTP,
		srv.StartRelayer,
		srv.RunFuzzer,
		srv.RunPprofServer,
		srv.PeriodicLogEngineOffset,
		srv.PeriodicGrpcUpdateStreamAgeBuckets,
	}

	g, groupCtx := errgroup.WithContext(ctx)

	for i, fn := range runFns {
		fn := fn
		g.Go(func() error {
			err := fn(groupCtx)
			if err != nil {
				slog.Error("[unisondb.main]",
					slog.String("event_type", "service.initialization.failed"),
					slog.Any("error", err),
					slog.Int("step", i),
				)
			}
			return err
		})
	}

	slog.Info("[unisondb.main]",
		slog.String("event_type", "service.initialization.completed"),
		slog.Group("service",
			slog.String("mode", mode),
			slog.String("env", env),
			slog.String("config_path", configPath)),
	)

	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("[unisondb.main]",
			slog.String("event_type", "service.shutdown.started"),
			slog.String("shutdown_reason", "errored"),
			slog.Any("error", err),
			slog.Group("service",
				slog.String("mode", mode),
				slog.String("env", env),
				slog.String("config_path", configPath)),
		)
	} else {
		slog.Info("[unisondb.main]",
			slog.String("event_type", "service.shutdown.started"),
			slog.String("shutdown_reason", "context.cancelled.signal"),
			slog.Group("service",
				slog.String("mode", mode),
				slog.String("env", env),
				slog.String("config_path", configPath)),
		)
	}

	// begin graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// reverse the order of shutdown
	slices.Reverse(srv.DeferCallback)
	for i, cb := range srv.DeferCallback {
		func(idx int, f func(context.Context)) {
			slog.Debug("[unisondb.main] executing shutdown callback", "index", idx)
			f(shutdownCtx)
		}(i, cb)
	}

	slog.Info("[unisondb.main]",
		slog.String("event_type", "service.shutdown.completed"),
		slog.Group("service",
			slog.String("mode", mode),
			slog.String("env", env),
			slog.String("config_path", configPath)),
	)
	return err
}
