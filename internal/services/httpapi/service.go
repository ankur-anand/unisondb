package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/gorilla/mux"
)

const (
	operationPut    = "put"
	operationDelete = "delete"

	// maxRequestBodySize is the maximum size of request body (1MB).
	maxRequestBodySize = 1 << 20
)

// Implements HTTP API for the Engine. All the Values are base64 encoded.

// Service implements HTTP API handlers for UnisonDB.
type Service struct {
	engines map[string]*dbkernel.Engine
	// map[string]*transactionState - txnID -> transaction state
	transactions   sync.Map
	healthResponse []byte
}

// transactionState holds the state of an active transaction.
type transactionState struct {
	txn       *dbkernel.Txn
	namespace string
	createdAt time.Time
	mu        sync.Mutex
}

// NewService creates a new HTTP API service.
func NewService(engines map[string]*dbkernel.Engine) *Service {
	namespaces := make([]string, 0, len(engines))
	for ns := range engines {
		namespaces = append(namespaces, ns)
	}

	healthResp := HealthResponse{
		Status:     "ok",
		Namespaces: namespaces,
	}

	healthJSON, _ := json.Marshal(healthResp)

	return &Service{
		engines:        engines,
		healthResponse: healthJSON,
	}
}

// RegisterRoutes registers all HTTP API routes with the given router.
func (s *Service) RegisterRoutes(router *mux.Router) {
	api := router.PathPrefix("/api/v1/{namespace}").Subrouter()

	// kv ops
	api.HandleFunc("/kv/{key}", s.handlePutKV).Methods(http.MethodPut)
	api.HandleFunc("/kv/{key}", s.handleGetKV).Methods(http.MethodGet)
	api.HandleFunc("/kv/{key}", s.handleDeleteKV).Methods(http.MethodDelete)
	api.HandleFunc("/kv/batch", s.handleBatchKV).Methods(http.MethodPost)

	// wide columns ops
	api.HandleFunc("/row/{rowKey}", s.handlePutRow).Methods(http.MethodPut)
	api.HandleFunc("/row/{rowKey}", s.handleGetRow).Methods(http.MethodGet)
	api.HandleFunc("/row/{rowKey}", s.handleDeleteRow).Methods(http.MethodDelete)
	api.HandleFunc("/row/{rowKey}/columns", s.handleDeleteColumns).Methods(http.MethodDelete)
	api.HandleFunc("/row/batch", s.handleBatchRows).Methods(http.MethodPost)

	// Lob get
	api.HandleFunc("/lob/{key}", s.handleGetLOB).Methods(http.MethodGet)

	// Transaction OPS
	api.HandleFunc("/tx/begin", s.handleBeginTransaction).Methods(http.MethodPost)
	api.HandleFunc("/tx/{txnId}/kv", s.handleAppendKV).Methods(http.MethodPost)
	api.HandleFunc("/tx/{txnId}/row", s.handleAppendRow).Methods(http.MethodPost)
	api.HandleFunc("/tx/{txnId}/lob", s.handleAppendLOB).Methods(http.MethodPost)
	api.HandleFunc("/tx/{txnId}/commit", s.handleCommitTransaction).Methods(http.MethodPost)
	api.HandleFunc("/tx/{txnId}/abort", s.handleAbortTransaction).Methods(http.MethodPost)

	// metadata ops
	api.HandleFunc("/offset", s.handleGetOffset).Methods(http.MethodGet)
	api.HandleFunc("/stats", s.handleGetStats).Methods(http.MethodGet)
	api.HandleFunc("/checkpoint", s.handleGetCheckpoint).Methods(http.MethodGet)
}

func (s *Service) getTransaction(txnID string) (*transactionState, error) {
	value, ok := s.transactions.Load(txnID)
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txnID)
	}
	return value.(*transactionState), nil
}

func (s *Service) storeTransaction(txnID string, state *transactionState) {
	s.transactions.Store(txnID, state)
}

func (s *Service) deleteTransaction(txnID string) {
	s.transactions.Delete(txnID)
}

func (s *Service) getEngine(namespace string) (*dbkernel.Engine, error) {
	engine, ok := s.engines[namespace]
	if !ok {
		return nil, fmt.Errorf("namespace not found: %s", namespace)
	}
	return engine, nil
}

func respondJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if data != nil {
		err := json.NewEncoder(w).Encode(data)
		if err != nil {
			slog.Error("[httpapi]: error encoding response", "err", err)
		}
	}
}

func respondError(w http.ResponseWriter, statusCode int, message string) {
	respondJSON(w, statusCode, map[string]string{"error": message})
}

type PutKVRequest struct {
	Value string `json:"value"`
}

type PutKVResponse struct {
	Success bool `json:"success"`
}

func (s *Service) handlePutKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	key := vars["key"]

	if key == "" {
		respondError(w, http.StatusBadRequest, "key cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	var req PutKVRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	value, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value: %v", err))
		return
	}

	if err := engine.PutKV([]byte(key), value); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to put KV: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, PutKVResponse{Success: true})
}

type GetKVResponse struct {
	// base64 encoded value
	Value string `json:"value"`
	Found bool   `json:"found"`
}

func (s *Service) handleGetKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	key := vars["key"]

	if key == "" {
		respondError(w, http.StatusBadRequest, "key cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	value, err := engine.GetKV([]byte(key))
	if err != nil {
		if errors.Is(err, dbkernel.ErrKeyNotFound) {
			respondJSON(w, http.StatusOK, GetKVResponse{Found: false})
			return
		}
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get KV: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, GetKVResponse{
		Value: base64.StdEncoding.EncodeToString(value),
		Found: true,
	})
}

type DeleteKVResponse struct {
	Success bool `json:"success"`
}

func (s *Service) handleDeleteKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	key := vars["key"]

	if key == "" {
		respondError(w, http.StatusBadRequest, "key cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := engine.DeleteKV([]byte(key)); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete KV: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, DeleteKVResponse{Success: true})
}

// BatchKVRequest represents batch operations request.
type BatchKVRequest struct {
	// "put" or "delete"
	Operation string        `json:"operation"`
	Items     []BatchKVItem `json:"items"`
	Keys      []string      `json:"keys"`
}

// BatchKVItem represents a single KV item in a batch.
type BatchKVItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// BatchKVResponse represents batch operations response.
type BatchKVResponse struct {
	Success bool `json:"success"`
	Count   int  `json:"count"`
}

// nolint:funlen
func (s *Service) handleBatchKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	var req BatchKVRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	switch req.Operation {
	case operationPut:
		if len(req.Items) == 0 {
			respondError(w, http.StatusBadRequest, "items cannot be empty")
			return
		}

		keys := make([][]byte, len(req.Items))
		values := make([][]byte, len(req.Items))

		for i, item := range req.Items {
			if item.Key == "" {
				respondError(w, http.StatusBadRequest, fmt.Sprintf("key at index %d cannot be empty", i))
				return
			}

			value, err := base64.StdEncoding.DecodeString(item.Value)
			if err != nil {
				respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value at index %d: %v", i, err))
				return
			}

			keys[i] = []byte(item.Key)
			values[i] = value
		}

		if err := engine.BatchPutKV(keys, values); err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to batch put: %v", err))
			return
		}

		respondJSON(w, http.StatusOK, BatchKVResponse{Success: true, Count: len(req.Items)})

	case operationDelete:
		if len(req.Keys) == 0 {
			respondError(w, http.StatusBadRequest, "keys cannot be empty")
			return
		}

		keys := make([][]byte, len(req.Keys))
		for i, key := range req.Keys {
			if key == "" {
				respondError(w, http.StatusBadRequest, fmt.Sprintf("key at index %d cannot be empty", i))
				return
			}
			keys[i] = []byte(key)
		}

		if err := engine.BatchDeleteKV(keys); err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to batch delete: %v", err))
			return
		}

		respondJSON(w, http.StatusOK, BatchKVResponse{Success: true, Count: len(req.Keys)})

	default:
		respondError(w, http.StatusBadRequest, "unknown operation: "+req.Operation)
	}
}

type PutRowRequest struct {
	// column name -> base64 encoded value
	Columns map[string]string `json:"columns"`
}

type PutRowResponse struct {
	Success bool `json:"success"`
}

func (s *Service) handlePutRow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	rowKey := vars["rowKey"]

	if rowKey == "" {
		respondError(w, http.StatusBadRequest, "rowKey cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	var req PutRowRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if len(req.Columns) == 0 {
		respondError(w, http.StatusBadRequest, "columns cannot be empty")
		return
	}

	columns := make(map[string][]byte)
	for colName, encodedValue := range req.Columns {
		value, err := base64.StdEncoding.DecodeString(encodedValue)
		if err != nil {
			respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value for column %s: %v", colName, err))
			return
		}
		columns[colName] = value
	}

	if err := engine.PutColumnsForRow([]byte(rowKey), columns); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to put row: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, PutRowResponse{Success: true})
}

type GetRowResponse struct {
	// column name -> base64 encoded value
	Columns map[string]string `json:"columns"`
	Found   bool              `json:"found"`
}

func (s *Service) handleGetRow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	rowKey := vars["rowKey"]

	if rowKey == "" {
		respondError(w, http.StatusBadRequest, "rowKey cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	// column filter from query params if present
	columnFilter := r.URL.Query().Get("columns")
	var predicate func(string) bool
	if columnFilter != "" {
		requestedColumns := strings.Split(columnFilter, ",")
		columnSet := make(map[string]bool)
		for _, col := range requestedColumns {
			columnSet[strings.TrimSpace(col)] = true
		}
		predicate = func(colName string) bool {
			return columnSet[colName]
		}
	} else {
		predicate = func(string) bool { return true }
	}

	columns, err := engine.GetRowColumns(rowKey, predicate)
	if err != nil {
		if errors.Is(err, dbkernel.ErrKeyNotFound) {
			respondJSON(w, http.StatusOK, GetRowResponse{Found: false, Columns: make(map[string]string)})
			return
		}
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get row: %v", err))
		return
	}

	encodedColumns := make(map[string]string)
	for colName, value := range columns {
		encodedColumns[colName] = base64.StdEncoding.EncodeToString(value)
	}

	respondJSON(w, http.StatusOK, GetRowResponse{
		Columns: encodedColumns,
		Found:   true,
	})
}

type DeleteRowResponse struct {
	Success bool `json:"success"`
}

func (s *Service) handleDeleteRow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	rowKey := vars["rowKey"]

	if rowKey == "" {
		respondError(w, http.StatusBadRequest, "rowKey cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := engine.DeleteRow([]byte(rowKey)); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete row: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, DeleteRowResponse{Success: true})
}

type DeleteColumnsRequest struct {
	// column name -> base64 encoded value
	Columns map[string]string `json:"columns"`
}

type DeleteColumnsResponse struct {
	Success bool `json:"success"`
}

func (s *Service) handleDeleteColumns(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	rowKey := vars["rowKey"]

	if rowKey == "" {
		respondError(w, http.StatusBadRequest, "rowKey cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	var req DeleteColumnsRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if len(req.Columns) == 0 {
		respondError(w, http.StatusBadRequest, "columns cannot be empty")
		return
	}

	columns := make(map[string][]byte)
	for colName, encodedValue := range req.Columns {
		value, err := base64.StdEncoding.DecodeString(encodedValue)
		if err != nil {
			respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value for column %s: %v", colName, err))
			return
		}
		columns[colName] = value
	}

	if err := engine.DeleteColumnsForRow([]byte(rowKey), columns); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete columns: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, DeleteColumnsResponse{Success: true})
}

type BatchRowsRequest struct {
	Operation string         `json:"operation"`
	Rows      []BatchRowItem `json:"rows"`
}

type BatchRowItem struct {
	RowKey  string            `json:"rowKey"`
	Columns map[string]string `json:"columns"`
}

type BatchRowsResponse struct {
	Success bool `json:"success"`
	Count   int  `json:"count"`
}

// nolint:funlen
func (s *Service) handleBatchRows(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	var req BatchRowsRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if len(req.Rows) == 0 {
		respondError(w, http.StatusBadRequest, "rows cannot be empty")
		return
	}

	switch req.Operation {
	case operationPut:
		rowKeys := make([][]byte, len(req.Rows))
		columnMaps := make([]map[string][]byte, len(req.Rows))

		for i, row := range req.Rows {
			if row.RowKey == "" {
				respondError(w, http.StatusBadRequest, fmt.Sprintf("rowKey at index %d cannot be empty", i))
				return
			}

			columns := make(map[string][]byte)
			for colName, encodedValue := range row.Columns {
				value, err := base64.StdEncoding.DecodeString(encodedValue)
				if err != nil {
					respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value for column %s at index %d: %v", colName, i, err))
					return
				}
				columns[colName] = value
			}

			rowKeys[i] = []byte(row.RowKey)
			columnMaps[i] = columns
		}

		if err := engine.PutColumnsForRows(rowKeys, columnMaps); err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to batch put rows: %v", err))
			return
		}

		respondJSON(w, http.StatusOK, BatchRowsResponse{Success: true, Count: len(req.Rows)})

	case operationDelete:
		rowKeys := make([][]byte, len(req.Rows))
		for i, row := range req.Rows {
			if row.RowKey == "" {
				respondError(w, http.StatusBadRequest, fmt.Sprintf("rowKey at index %d cannot be empty", i))
				return
			}
			rowKeys[i] = []byte(row.RowKey)
		}

		if err := engine.BatchDeleteRows(rowKeys); err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to batch delete rows: %v", err))
			return
		}

		respondJSON(w, http.StatusOK, BatchRowsResponse{Success: true, Count: len(req.Rows)})

	default:
		respondError(w, http.StatusBadRequest, "unknown operation: "+req.Operation)
	}
}

func (s *Service) handleGetLOB(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	key := vars["key"]

	if key == "" {
		respondError(w, http.StatusBadRequest, "key cannot be empty")
		return
	}

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	value, err := engine.GetLOB([]byte(key))
	if err != nil {
		if errors.Is(err, dbkernel.ErrKeyNotFound) {
			respondError(w, http.StatusNotFound, "key not found")
			return
		}
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get LOB: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(value)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(value); err != nil {
		slog.Error("[httpapi]: error writing LOB response", "err", err)
	}
}

type OffsetResponse struct {
	SegmentID uint32 `json:"segmentId"`
	Offset    int64  `json:"offset"`
}

func (s *Service) handleGetOffset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	offset := engine.CurrentOffset()
	respondJSON(w, http.StatusOK, OffsetResponse{
		SegmentID: offset.SegmentID,
		Offset:    offset.Offset,
	})
}

type StatsResponse struct {
	Namespace      string `json:"namespace"`
	OpsReceived    uint64 `json:"opsReceived"`
	OpsFlushed     uint64 `json:"opsFlushed"`
	CurrentSegment uint32 `json:"currentSegment"`
	CurrentOffset  int64  `json:"currentOffset"`
}

func (s *Service) handleGetStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	offset := engine.CurrentOffset()
	respondJSON(w, http.StatusOK, StatsResponse{
		Namespace:      engine.Namespace(),
		OpsReceived:    engine.OpsReceivedCount(),
		OpsFlushed:     engine.OpsFlushedCount(),
		CurrentSegment: offset.SegmentID,
		CurrentOffset:  offset.Offset,
	})
}

type CheckpointResponse struct {
	RecordProcessed uint64 `json:"recordProcessed"`
	SegmentID       uint32 `json:"segmentId"`
	Offset          int64  `json:"offset"`
}

func (s *Service) handleGetCheckpoint(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	metadata, err := engine.GetWalCheckPoint()
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get checkpoint: %v", err))
		return
	}

	var recordProcessed uint64
	var segmentID uint32
	var offset int64

	if metadata != nil {
		recordProcessed = metadata.RecordProcessed
		if metadata.Pos != nil {
			segmentID = metadata.Pos.SegmentID
			offset = metadata.Pos.Offset
		}
	}

	respondJSON(w, http.StatusOK, CheckpointResponse{
		RecordProcessed: recordProcessed,
		SegmentID:       segmentID,
		Offset:          offset,
	})
}

type HealthResponse struct {
	Status     string   `json:"status"`
	Namespaces []string `json:"namespaces"`
}

// HandleHealth handles the /health endpoint for server health checks.
func (s *Service) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(s.healthResponse); err != nil {
		slog.Error("[httpapi]: error writing health response", "err", err)
	}
}
