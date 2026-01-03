package cliapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/ankur-anand/unisondb/internal/services/httpapi"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPService serves the HTTP API and metrics endpoints.
type HTTPService struct {
	server     *http.Server
	httpAPISvc *httpapi.Service
	addr       string
	boundAddr  string
	ready      chan struct{}
}

func (h *HTTPService) Name() string {
	return "http"
}

func (h *HTTPService) Setup(ctx context.Context, deps *Dependencies) error {
	h.ready = make(chan struct{})

	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler()).Methods(http.MethodGet)

	if deps.FuzzStats != nil {
		router.Handle("/fuzzstats", deps.FuzzStats).Methods(http.MethodGet)
	}

	h.httpAPISvc = httpapi.NewService(deps.Engines)
	router.HandleFunc("/health", h.httpAPISvc.HandleHealth).Methods(http.MethodGet)
	h.httpAPISvc.RegisterRoutes(router)

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "HTTP.API.registered"),
		slog.Int("namespace_count", len(deps.Engines)),
	)

	ip := deps.Config.ListenIP
	if ip == "" {
		ip = "0.0.0.0"
	}
	h.addr = fmt.Sprintf("%s:%d", ip, deps.Config.HTTPPort)

	h.server = &http.Server{
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      router,
	}

	return nil
}

func (h *HTTPService) Run(ctx context.Context) error {
	var lis net.ListenConfig
	l, err := lis.Listen(ctx, "tcp", h.addr)
	if err != nil {
		return fmt.Errorf("http listen error: %w", err)
	}

	h.boundAddr = l.Addr().String()
	close(h.ready)

	slog.Info("[unisondb.cliapp]",
		slog.String("event_type", "HTTP.server.started"),
		slog.String("addr", h.boundAddr),
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- h.server.Serve(l)
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

func (h *HTTPService) Close(ctx context.Context) error {
	if h.httpAPISvc != nil {
		h.httpAPISvc.Close()
	}

	if h.server != nil {
		slog.Info("[unisondb.cliapp]",
			slog.String("event_type", "stopping.HTTP.server"))

		if err := h.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http shutdown error: %w", err)
		}
	}
	return nil
}

func (h *HTTPService) Addr() string {
	return h.addr
}

func (h *HTTPService) BoundAddr() string {
	return h.boundAddr
}

func (h *HTTPService) Ready() <-chan struct{} {
	return h.ready
}
