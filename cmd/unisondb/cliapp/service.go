package cliapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"golang.org/x/sync/errgroup"
)

type Service interface {
	Name() string
	Setup(ctx context.Context, deps *Dependencies) error
	Run(ctx context.Context) error
	Close(ctx context.Context) error
}

type PortReporter interface {
	Service
	BoundAddr() string
	Ready() <-chan struct{}
}

// Register adds a service to the server.
// Services are setup in registration order and closed in reverse order.
func (ms *Server) Register(svc Service) {
	ms.services = append(ms.services, svc)
	if ms.deps == nil {
		return
	}
	if raftSvc, ok := svc.(*RaftService); ok {
		ms.deps.RaftService = raftSvc
	}
}

func (ms *Server) SetupServices(ctx context.Context) error {
	for _, svc := range ms.services {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "service.setup.started"),
			slog.String("service", svc.Name()))

		if err := svc.Setup(ctx, ms.deps); err != nil {
			return fmt.Errorf("service %s setup failed: %w", svc.Name(), err)
		}
	}
	return nil
}

func (ms *Server) RunServices(ctx context.Context) error {
	g, groupCtx := errgroup.WithContext(ctx)

	for _, svc := range ms.services {
		svc := svc
		g.Go(func() error {
			slog.Info("[unisondb.cliapp]",
				slog.String("event_type", "service.run.started"),
				slog.String("service", svc.Name()))

			err := svc.Run(groupCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("[unisondb.cliapp]",
					slog.String("event_type", "service.run.error"),
					slog.String("service", svc.Name()),
					slog.Any("error", err))
			}
			return err
		})
	}

	if ms.PortsFile != "" {
		go ms.waitAndWritePortsFile(ctx)
	}

	return g.Wait()
}

// CloseServices shuts down all services in reverse order.
func (ms *Server) CloseServices(ctx context.Context) {
	for i := len(ms.services) - 1; i >= 0; i-- {
		svc := ms.services[i]
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "service.close.started"),
			slog.String("service", svc.Name()))

		if err := svc.Close(ctx); err != nil {
			slog.Error("[unisondb.cliapp]",
				slog.String("event_type", "service.close.error"),
				slog.String("service", svc.Name()),
				slog.Any("error", err))
		}
	}
}

func (ms *Server) BuildDeps() *Dependencies {
	ms.deps = &Dependencies{
		Mode:          ms.mode,
		Env:           ms.env,
		Config:        ms.cfg,
		Engines:       ms.engines,
		StorageConfig: ms.storageConfig,
		Notifiers:     ms.notifiers,
		Logger:        ms.pl,
		FuzzStats:     ms.fuzzStats,
	}
	return ms.deps
}

func (ms *Server) waitAndWritePortsFile(ctx context.Context) {
	ports := make(map[string]string)

	for _, svc := range ms.services {
		if pr, ok := svc.(PortReporter); ok {
			select {
			case <-pr.Ready():
				if addr := pr.BoundAddr(); addr != "" {
					ports[svc.Name()] = addr
				}
			case <-ctx.Done():
				slog.Warn("[unisondb.cliapp] context cancelled while waiting for ports",
					slog.String("service", svc.Name()))
				return
			}
		}
	}

	data, err := json.MarshalIndent(ports, "", "  ")
	if err != nil {
		slog.Error("[unisondb.cliapp] failed to marshal ports file",
			slog.Any("error", err))
		return
	}

	if err := os.WriteFile(ms.PortsFile, data, 0o644); err != nil {
		slog.Error("[unisondb.cliapp] failed to write ports file",
			slog.String("path", ms.PortsFile),
			slog.Any("error", err))
		return
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "ports.file.written"),
		slog.String("path", ms.PortsFile),
		slog.Int("service_count", len(ports)))
}
