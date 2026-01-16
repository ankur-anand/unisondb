package cliapp

import (
	"context"
	"encoding/json"
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
	server      *http.Server
	httpAPISvc  *httpapi.Service
	addr        string
	boundAddr   string
	ready       chan struct{}
	raftService *RaftService
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
	router.HandleFunc("/healthz", h.httpAPISvc.HandleHealth).Methods(http.MethodGet)
	h.httpAPISvc.RegisterRoutes(router)
	h.raftService = deps.RaftService
	router.HandleFunc("/readyz", h.handleReadyz).Methods(http.MethodGet)
	router.HandleFunc("/status/leader", h.handleRaftLeader).Methods(http.MethodGet)
	router.HandleFunc("/status/peers", h.handleRaftPeers).Methods(http.MethodGet)

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

type raftLeaderStatus struct {
	Namespace  string `json:"namespace,omitempty"`
	LeaderAddr string `json:"leader_addr"`
	LeaderID   string `json:"leader_id"`
}

type raftPeersStatus struct {
	Namespace string   `json:"namespace,omitempty"`
	Peers     []string `json:"peers"`
}

type raftReadyStatus struct {
	Status     string `json:"status"`
	Namespace  string `json:"namespace,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
	LeaderID   string `json:"leader_id,omitempty"`
}

func (h *HTTPService) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if h.raftService == nil || !h.raftService.enabled {
		writeJSON(w, http.StatusOK, raftReadyStatus{Status: "ready"})
		return
	}

	if len(h.raftService.clusters) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, raftReadyStatus{Status: "not_ready"})
		return
	}

	namespace := r.URL.Query().Get("namespace")
	if namespace == "" {
		if len(h.raftService.clusters) > 1 {
			writeJSONError(w, http.StatusBadRequest, "namespace query param required when multiple namespaces are configured")
			return
		}
		for ns := range h.raftService.clusters {
			namespace = ns
			break
		}
	}

	nr, ok := h.raftService.clusters[namespace]
	if !ok || nr == nil || nr.cluster == nil {
		writeJSONError(w, http.StatusNotFound, "namespace not found: "+namespace)
		return
	}

	leaderAddr, leaderID := nr.cluster.LeaderWithID()
	if !nr.cluster.IsLeader() {
		writeJSON(w, http.StatusServiceUnavailable, raftReadyStatus{
			Status:     "not_leader",
			Namespace:  namespace,
			LeaderAddr: string(leaderAddr),
			LeaderID:   string(leaderID),
		})
		return
	}

	writeJSON(w, http.StatusOK, raftReadyStatus{
		Status:     "ready",
		Namespace:  namespace,
		LeaderAddr: string(leaderAddr),
		LeaderID:   string(leaderID),
	})
}

func (h *HTTPService) handleRaftLeader(w http.ResponseWriter, r *http.Request) {
	if h.raftService == nil || !h.raftService.enabled {
		writeJSONError(w, http.StatusServiceUnavailable, "raft service disabled")
		return
	}

	namespace := r.URL.Query().Get("namespace")
	if namespace != "" {
		nr, ok := h.raftService.clusters[namespace]
		if !ok || nr == nil || nr.cluster == nil {
			writeJSONError(w, http.StatusNotFound, "namespace not found: "+namespace)
			return
		}
		leaderAddr, leaderID := nr.cluster.LeaderWithID()
		writeJSON(w, http.StatusOK, raftLeaderStatus{
			Namespace:  namespace,
			LeaderAddr: string(leaderAddr),
			LeaderID:   string(leaderID),
		})
		return
	}

	leaders := make(map[string]raftLeaderStatus, len(h.raftService.clusters))
	for ns, nr := range h.raftService.clusters {
		if nr == nil || nr.cluster == nil {
			continue
		}
		leaderAddr, leaderID := nr.cluster.LeaderWithID()
		leaders[ns] = raftLeaderStatus{
			LeaderAddr: string(leaderAddr),
			LeaderID:   string(leaderID),
		}
	}
	writeJSON(w, http.StatusOK, map[string]map[string]raftLeaderStatus{
		"leaders": leaders,
	})
}

func (h *HTTPService) handleRaftPeers(w http.ResponseWriter, r *http.Request) {
	if h.raftService == nil || !h.raftService.enabled {
		writeJSONError(w, http.StatusServiceUnavailable, "raft service disabled")
		return
	}

	namespace := r.URL.Query().Get("namespace")
	if namespace != "" {
		nr, ok := h.raftService.clusters[namespace]
		if !ok || nr == nil || nr.cluster == nil {
			writeJSONError(w, http.StatusNotFound, "namespace not found: "+namespace)
			return
		}
		cfg, err := nr.cluster.RaftConfiguration()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("get raft config: %v", err))
			return
		}
		peers := make([]string, 0, len(cfg.Servers))
		for _, server := range cfg.Servers {
			peers = append(peers, string(server.Address))
		}
		writeJSON(w, http.StatusOK, raftPeersStatus{
			Namespace: namespace,
			Peers:     peers,
		})
		return
	}

	peersByNamespace := make(map[string][]string, len(h.raftService.clusters))
	for ns, nr := range h.raftService.clusters {
		if nr == nil || nr.cluster == nil {
			continue
		}
		cfg, err := nr.cluster.RaftConfiguration()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("get raft config: %v", err))
			return
		}
		peers := make([]string, 0, len(cfg.Servers))
		for _, server := range cfg.Servers {
			peers = append(peers, string(server.Address))
		}
		peersByNamespace[ns] = peers
	}
	writeJSON(w, http.StatusOK, map[string]map[string][]string{
		"peers": peersByNamespace,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if payload == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("[unisondb.cliapp] failed to encode response", slog.Any("error", err))
	}
}

func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]string{"error": message})
}
