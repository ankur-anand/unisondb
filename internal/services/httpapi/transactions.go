package httpapi

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ankur-anand/unisondb/schemas/logrecord"
	"github.com/gorilla/mux"
)

type BeginTransactionRequest struct {
	Operation string `json:"operation"`
	EntryType string `json:"entryType"`
}

type BeginTransactionResponse struct {
	TxnID   string `json:"txnId"`
	Success bool   `json:"success"`
}

type AppendKVRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type AppendRowRequest struct {
	RowKey string `json:"rowKey"`
	// column name -> base64 encoded value
	Columns map[string]string `json:"columns"`
}

type CommitTransactionResponse struct {
	Success  bool   `json:"success"`
	Checksum uint32 `json:"checksum,omitempty"`
}

func (s *Service) handleBeginTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]

	engine, err := s.getEngine(namespace)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	var req BeginTransactionRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	var opType logrecord.LogOperationType
	switch req.Operation {
	case operationPut:
		opType = logrecord.LogOperationTypeInsert
	case operationDelete:
		opType = logrecord.LogOperationTypeDelete
	default:
		respondError(w, http.StatusBadRequest, "unknown operation: "+req.Operation)
		return
	}

	// Parse entry type
	var entryType logrecord.LogEntryType
	switch req.EntryType {
	case "kv":
		entryType = logrecord.LogEntryTypeKV
	case "row":
		entryType = logrecord.LogEntryTypeRow
	case "lob":
		entryType = logrecord.LogEntryTypeChunked
	default:
		respondError(w, http.StatusBadRequest, "unknown entry type: "+req.EntryType)
		return
	}

	txn, err := engine.NewTxn(opType, entryType)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create transaction: %v", err))
		return
	}

	txnIDBytes := txn.TxnID()
	txnID := hex.EncodeToString(txnIDBytes)

	state := &transactionState{
		txn:       txn,
		namespace: namespace,
		createdAt: time.Now(),
	}
	s.storeTransaction(txnID, state)

	respondJSON(w, http.StatusOK, BeginTransactionResponse{
		TxnID:   txnID,
		Success: true,
	})
}

func (s *Service) handleAppendKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txnID := vars["txnId"]

	state, err := s.getTransaction(txnID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	var req AppendKVRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if req.Key == "" {
		respondError(w, http.StatusBadRequest, "key cannot be empty")
		return
	}

	value, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid base64 value: %v", err))
		return
	}

	if err := state.txn.AppendKVTxn([]byte(req.Key), value); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to append to transaction: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (s *Service) handleAppendRow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txnID := vars["txnId"]

	// Get transaction
	state, err := s.getTransaction(txnID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	var req AppendRowRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if req.RowKey == "" {
		respondError(w, http.StatusBadRequest, "rowKey cannot be empty")
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

	if err := state.txn.AppendColumnTxn([]byte(req.RowKey), columns); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to append to transaction: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (s *Service) handleAppendLOB(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txnID := vars["txnId"]

	state, err := s.getTransaction(txnID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	key := r.URL.Query().Get("key")
	if key == "" {
		respondError(w, http.StatusBadRequest, "key query parameter is required")
		return
	}

	// Limit request body size to 1MB
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

	// Read body in chunks and append to transaction
	buffer := make([]byte, 32*1024) // 32KB chunks
	for {
		n, err := r.Body.Read(buffer)
		if n > 0 {
			if err := state.txn.AppendKVTxn([]byte(key), buffer[:n]); err != nil {
				respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to append to transaction: %v", err))
				return
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read request body: %v", err))
			return
		}
	}

	respondJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (s *Service) handleCommitTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txnID := vars["txnId"]

	state, err := s.getTransaction(txnID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if err := state.txn.Commit(); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to commit transaction: %v", err))
		return
	}

	checksum := state.txn.ChunkedValueChecksum()

	s.deleteTransaction(txnID)

	respondJSON(w, http.StatusOK, CommitTransactionResponse{
		Success:  true,
		Checksum: checksum,
	})
}

func (s *Service) handleAbortTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txnID := vars["txnId"]

	_, err := s.getTransaction(txnID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	s.deleteTransaction(txnID)

	respondJSON(w, http.StatusOK, map[string]bool{"success": true})
}
