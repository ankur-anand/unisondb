package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ankur-anand/unisondb/cmd/unisondb/cliapp"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

func main() {
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
				Usage:   "Enable gRPC server",
				EnvVars: []string{"UNISON_GRPC_ENABLED"},
				Value:   false,
			},
		},

		Commands: []*cli.Command{
			{
				Name:  "replicator",
				Usage: "Run in replicator mode",
				Action: func(c *cli.Context) error {
					return Run(c.Context, c.String("config"), c.String("env"), "replicator")
				},
			},
			{
				Name:  "relayer",
				Usage: "Run in relayer mode",
				Action: func(c *cli.Context) error {
					return Run(c.Context, c.String("config"), c.String("env"), "relayer")
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("[unisondb.main] failed to start", "error", err)
		os.Exit(1)
	}
}

func Run(_ context.Context, configPath, env, mode string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	srv := cliapp.Server{}

	slog.Info("[unisondb.main] initializing", "mode", mode, "env", env)

	setup := []func(context.Context) error{
		srv.InitFromCLI(configPath, env, mode),
		srv.InitTelemetry,
		srv.SetupStorageConfig,
		srv.SetupStorage,
		srv.SetupGrpcServer,
		srv.SetupHTTPServer,
		srv.SetupRelayer,
	}

	for i, fn := range setup {
		if err := fn(ctx); err != nil {
			slog.Error("[unisondb.main] setup failed", "step", i, "error", err)
			return err
		}
	}

	slog.Info("[unisondb.main] started", "mode", mode, "env", env)

	// Run services concurrently
	runFns := []func(context.Context) error{
		srv.RunGrpc,
		srv.RunHTTP,
		srv.StartRelayer,
	}

	g, groupCtx := errgroup.WithContext(ctx)

	for i, fn := range runFns {
		fn := fn
		g.Go(func() error {
			err := fn(groupCtx)
			if err != nil {
				slog.Error("[unisondb.main] run function failed", "index", i, "error", err)
			}
			return err
		})
	}

	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("[unisondb.main] shutting down due to error", "error", err)
	} else {
		slog.Info("[unisondb.main] context cancelled, initiating shutdown", "mode", mode, "env", env)
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	for i, cb := range srv.DeferCallback {
		func(idx int, f func(context.Context)) {
			slog.Debug("[unisondb.main] executing shutdown callback", "index", idx)
			f(shutdownCtx)
		}(i, cb)
	}

	slog.Info("[unisondb.main] shutdown complete", "mode", mode)
	return err
}
