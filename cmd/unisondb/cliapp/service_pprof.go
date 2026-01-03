package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"
)

type PProfService struct {
	server    *http.Server
	boundAddr string
	ready     chan struct{}
	enabled   bool
}

func (p *PProfService) Name() string {
	return "pprof"
}

func (p *PProfService) Setup(ctx context.Context, deps *Dependencies) error {
	p.ready = make(chan struct{})

	if !deps.Config.PProfConfig.Enabled {
		p.enabled = false
		close(p.ready)
		return nil
	}
	p.enabled = true

	p.server = &http.Server{
		Addr:         fmt.Sprintf("localhost:%d", deps.Config.PProfConfig.Port),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
		Handler:      http.DefaultServeMux,
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "pprof.setup.completed"),
		slog.Int("port", deps.Config.PProfConfig.Port),
	)

	return nil
}

func (p *PProfService) Run(ctx context.Context) error {
	if !p.enabled {
		return nil
	}

	l, err := net.Listen("tcp", p.server.Addr)
	if err != nil {
		return fmt.Errorf("pprof listen error: %w", err)
	}

	p.boundAddr = l.Addr().String()
	close(p.ready)

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "pprof.HTTP.server.started"),
		slog.String("addr", p.boundAddr),
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.server.Serve(l)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}

func (p *PProfService) Close(ctx context.Context) error {
	if p.server == nil {
		return nil
	}

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "stopping.pprof.HTTP.server"))

	if err := p.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("pprof shutdown error: %w", err)
	}

	return nil
}

func (p *PProfService) Enabled() bool {
	return p.enabled
}

func (p *PProfService) BoundAddr() string {
	return p.boundAddr
}

func (p *PProfService) Ready() <-chan struct{} {
	return p.ready
}
