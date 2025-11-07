package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testServer struct {
	service    *Service
	router     *mux.Router
	engine     *dbkernel.Engine
	backupRoot string
}

func setupTestServer(t *testing.T) (*testServer, func()) {
	return setupTestServerWithConfig(t, nil)
}

func setupTestServerWithConfig(t *testing.T, modify func(cfg *dbkernel.EngineConfig)) (*testServer, func()) {
	t.Helper()

	tmpDir := t.TempDir()

	cfg := dbkernel.NewDefaultEngineConfig()
	cfg.DBEngine = dbkernel.LMDBEngine
	if modify != nil {
		modify(cfg)
	}

	engine, err := dbkernel.NewStorageEngine(tmpDir, "test", cfg)
	require.NoError(t, err)

	engines := map[string]*dbkernel.Engine{
		"test": engine,
	}

	service := NewService(engines)
	router := mux.NewRouter()
	router.HandleFunc("/health", service.HandleHealth).Methods(http.MethodGet)
	service.RegisterRoutes(router)

	cleanup := func() {
		engine.Close(context.Background())
	}

	return &testServer{
		service:    service,
		router:     router,
		engine:     engine,
		backupRoot: filepath.Join(tmpDir, dbkernel.BackupRootDirName, "test"),
	}, cleanup
}

func makeRequest(t *testing.T, router *mux.Router, method, url string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(jsonBody)
	}

	req := httptest.NewRequest(method, url, reqBody)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	return rr
}

func encodeBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func decodeBase64(t *testing.T, data string) []byte {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(data)
	require.NoError(t, err)
	return decoded
}

type writeErrorRecorder struct {
	http.ResponseWriter
}

func (w *writeErrorRecorder) Write(p []byte) (int, error) {
	return 0, errors.New("write error")
}

func TestRespondJSON_EncodeError(t *testing.T) {
	rr := httptest.NewRecorder()
	payload := map[string]interface{}{
		"value": math.Inf(1),
	}

	respondJSON(rr, http.StatusCreated, payload)

	assert.Equal(t, http.StatusCreated, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	assert.Equal(t, "", rr.Body.String())
}

func TestPutKV_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := PutKVRequest{
		Value: encodeBase64([]byte("test-value")),
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/kv/test-key", reqBody)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp PutKVResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	value, err := ts.engine.GetKV([]byte("test-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value"), value)
}

func TestPutKV_EmptyKey(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := PutKVRequest{
		Value: encodeBase64([]byte("test-value")),
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/kv/", reqBody)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestPutKV_InvalidBase64(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := PutKVRequest{
		Value: "not-valid-base64!!!",
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/kv/test-key", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "invalid base64")
}

func TestPutKV_InvalidNamespace(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := PutKVRequest{
		Value: encodeBase64([]byte("test-value")),
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/nonexistent/kv/test-key", reqBody)

	assert.Equal(t, http.StatusNotFound, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "namespace not found")
}

func TestGetKV_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	err := ts.engine.PutKV([]byte("test-key"), []byte("test-value"))
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/kv/test-key", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp GetKVResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Found)
	assert.Equal(t, []byte("test-value"), decodeBase64(t, resp.Value))
}

func TestGetKV_NotFound(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/kv/nonexistent-key", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp GetKVResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.False(t, resp.Found)
}

func TestDeleteKV_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	err := ts.engine.PutKV([]byte("test-key"), []byte("test-value"))
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodDelete, "/api/v1/test/kv/test-key", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp DeleteKVResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	_, err = ts.engine.GetKV([]byte("test-key"))
	assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound)
}

func TestBatchKV_PutSuccess(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: "put",
		Items: []BatchKVItem{
			{Key: "key1", Value: encodeBase64([]byte("value1"))},
			{Key: "key2", Value: encodeBase64([]byte("value2"))},
			{Key: "key3", Value: encodeBase64([]byte("value3"))},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BatchKVResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, 3, resp.Count)

	for i := 1; i <= 3; i++ {
		key := []byte("key" + string(rune('0'+i)))
		expectedValue := []byte("value" + string(rune('0'+i)))
		value, err := ts.engine.GetKV(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

func TestBatchKV_DeleteSuccess(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	err := ts.engine.BatchPutKV(keys, values)
	require.NoError(t, err)

	reqBody := BatchKVRequest{
		Operation: "delete",
		Keys:      []string{"key1", "key2", "key3"},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BatchKVResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, 3, resp.Count)

	for _, key := range keys {
		_, err := ts.engine.GetKV(key)
		assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound)
	}
}

func TestBatchKV_InvalidOperation(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: "invalid",
		Items:     []BatchKVItem{{Key: "key1", Value: encodeBase64([]byte("value1"))}},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "unknown operation")
}

func TestBatchKV_PutEmptyItems(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: operationPut,
		Items:     []BatchKVItem{},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "items cannot be empty")
}

func TestBatchKV_PutInvalidBase64(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: operationPut,
		Items: []BatchKVItem{
			{Key: "key1", Value: "not-valid-base64!!!"},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "invalid base64 value at index 0")
}

func TestBatchKV_DeleteEmptyKeys(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: operationDelete,
		Keys:      []string{},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "keys cannot be empty")
}

func TestBatchKV_DeleteEmptyKeyEntry(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: operationDelete,
		Keys:      []string{"", "key2"},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "key at index 0 cannot be empty")
}

func TestPutRow_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := PutRowRequest{
		Columns: map[string]string{
			"col1": encodeBase64([]byte("value1")),
			"col2": encodeBase64([]byte("value2")),
			"col3": encodeBase64([]byte("value3")),
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/row/row1", reqBody)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp PutRowResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	columns, err := ts.engine.GetRowColumns("row1", func(string) bool { return true })
	require.NoError(t, err)
	assert.Len(t, columns, 3)
	assert.Equal(t, []byte("value1"), columns["col1"])
	assert.Equal(t, []byte("value2"), columns["col2"])
	assert.Equal(t, []byte("value3"), columns["col3"])
}

func TestPutRow_EmptyColumns(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := PutRowRequest{
		Columns: map[string]string{},
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/row/row1", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "columns cannot be empty")
}

func TestGetRow_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	columns := map[string][]byte{
		"col1": []byte("value1"),
		"col2": []byte("value2"),
		"col3": []byte("value3"),
	}
	err := ts.engine.PutColumnsForRow([]byte("row1"), columns)
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/row/row1", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp GetRowResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Found)
	assert.Len(t, resp.Columns, 3)
	assert.Equal(t, []byte("value1"), decodeBase64(t, resp.Columns["col1"]))
	assert.Equal(t, []byte("value2"), decodeBase64(t, resp.Columns["col2"]))
	assert.Equal(t, []byte("value3"), decodeBase64(t, resp.Columns["col3"]))
}

func TestGetRow_WithColumnFilter(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	columns := map[string][]byte{
		"col1": []byte("value1"),
		"col2": []byte("value2"),
		"col3": []byte("value3"),
	}
	err := ts.engine.PutColumnsForRow([]byte("row1"), columns)
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/row/row1?columns=col1,col3", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp GetRowResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Found)
	assert.Len(t, resp.Columns, 2)
	assert.Equal(t, []byte("value1"), decodeBase64(t, resp.Columns["col1"]))
	assert.Equal(t, []byte("value3"), decodeBase64(t, resp.Columns["col3"]))
	_, exists := resp.Columns["col2"]
	assert.False(t, exists)
}

func TestGetRow_NotFound(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/row/nonexistent", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp GetRowResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.False(t, resp.Found)
}

func TestDeleteRow_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	columns := map[string][]byte{
		"col1": []byte("value1"),
		"col2": []byte("value2"),
	}
	err := ts.engine.PutColumnsForRow([]byte("row1"), columns)
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodDelete, "/api/v1/test/row/row1", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp DeleteRowResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	_, err = ts.engine.GetRowColumns("row1", func(string) bool { return true })
	assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound)
}

func TestDeleteColumns_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	columns := map[string][]byte{
		"col1": []byte("value1"),
		"col2": []byte("value2"),
		"col3": []byte("value3"),
	}
	err := ts.engine.PutColumnsForRow([]byte("row1"), columns)
	require.NoError(t, err)

	reqBody := DeleteColumnsRequest{
		Columns: map[string]string{
			"col1": encodeBase64([]byte("value1")),
			"col2": encodeBase64([]byte("value2")),
		},
	}

	rr := makeRequest(t, ts.router, http.MethodDelete, "/api/v1/test/row/row1/columns", reqBody)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp DeleteColumnsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	remainingColumns, err := ts.engine.GetRowColumns("row1", func(string) bool { return true })
	require.NoError(t, err)
	assert.Len(t, remainingColumns, 1)
	assert.Equal(t, []byte("value3"), remainingColumns["col3"])
}

func TestBatchRows_PutSuccess(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchRowsRequest{
		Operation: "put",
		Rows: []BatchRowItem{
			{
				RowKey: "row1",
				Columns: map[string]string{
					"col1": encodeBase64([]byte("value1")),
					"col2": encodeBase64([]byte("value2")),
				},
			},
			{
				RowKey: "row2",
				Columns: map[string]string{
					"colA": encodeBase64([]byte("valueA")),
					"colB": encodeBase64([]byte("valueB")),
				},
			},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BatchRowsResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, 2, resp.Count)

	row1, err := ts.engine.GetRowColumns("row1", func(string) bool { return true })
	require.NoError(t, err)
	assert.Len(t, row1, 2)

	row2, err := ts.engine.GetRowColumns("row2", func(string) bool { return true })
	require.NoError(t, err)
	assert.Len(t, row2, 2)
}

func TestBatchRows_DeleteSuccess(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	rowKeys := [][]byte{[]byte("row1"), []byte("row2")}
	columnMaps := []map[string][]byte{
		{"col1": []byte("value1")},
		{"col2": []byte("value2")},
	}
	err := ts.engine.PutColumnsForRows(rowKeys, columnMaps)
	require.NoError(t, err)

	reqBody := BatchRowsRequest{
		Operation: "delete",
		Rows: []BatchRowItem{
			{RowKey: "row1"},
			{RowKey: "row2"},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BatchRowsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, 2, resp.Count)

	_, err = ts.engine.GetRowColumns("row1", func(string) bool { return true })
	assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound)

	_, err = ts.engine.GetRowColumns("row2", func(string) bool { return true })
	assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound)
}

func TestBatchRows_EmptyRows(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchRowsRequest{
		Operation: operationPut,
		Rows:      []BatchRowItem{},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "rows cannot be empty")
}

func TestBatchRows_RowKeyEmpty(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchRowsRequest{
		Operation: operationPut,
		Rows: []BatchRowItem{
			{
				RowKey: "",
				Columns: map[string]string{
					"col1": encodeBase64([]byte("value1")),
				},
			},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "rowKey at index 0 cannot be empty")
}

func TestBatchRows_InvalidBase64(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchRowsRequest{
		Operation: operationPut,
		Rows: []BatchRowItem{
			{
				RowKey: "row1",
				Columns: map[string]string{
					"col1": "not-valid-base64!!!",
				},
			},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "invalid base64 value for column col1 at index 0")
}

func TestBatchRows_DeleteEmptyRowKey(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BatchRowsRequest{
		Operation: operationDelete,
		Rows: []BatchRowItem{
			{RowKey: ""},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "rowKey at index 0 cannot be empty")
}

func TestGetLOB_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	largeData := make([]byte, 50*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	txn, err := ts.engine.NewTxn(1, 1)
	require.NoError(t, err)

	err = txn.AppendKVTxn([]byte("large-key"), largeData)
	require.NoError(t, err)

	err = txn.Commit()
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/test/lob/large-key", nil)
	rr := httptest.NewRecorder()
	ts.router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/octet-stream", rr.Header().Get("Content-Type"))

	responseData, err := io.ReadAll(rr.Body)
	require.NoError(t, err)
	assert.Equal(t, largeData, responseData)
}

func TestGetLOB_NotFound(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/test/lob/nonexistent", nil)
	rr := httptest.NewRecorder()
	ts.router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetLOB_WriteError(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	data := []byte("lob-data")

	txn, err := ts.engine.NewTxn(1, 1)
	require.NoError(t, err)

	err = txn.AppendKVTxn([]byte("lob-key"), data)
	require.NoError(t, err)

	err = txn.Commit()
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/test/lob/lob-key", nil)
	req = mux.SetURLVars(req, map[string]string{
		"namespace": "test",
		"key":       "lob-key",
	})

	rec := httptest.NewRecorder()
	w := &writeErrorRecorder{ResponseWriter: rec}

	ts.service.handleGetLOB(w, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	assert.Equal(t, "", rec.Body.String())
}

func TestGetOffset_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	err := ts.engine.PutKV([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/offset", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp OffsetResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.SegmentID, uint32(0))
	assert.GreaterOrEqual(t, resp.Offset, int64(0))
}

func TestGetStats_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	err := ts.engine.PutKV([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	err = ts.engine.PutKV([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/stats", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp StatsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "test", resp.Namespace)
	assert.GreaterOrEqual(t, resp.OpsReceived, uint64(2))
	assert.GreaterOrEqual(t, resp.OpsFlushed, uint64(0))
	assert.GreaterOrEqual(t, resp.CurrentSegment, uint32(0))
}

func TestGetCheckpoint_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	err := ts.engine.PutKV([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/checkpoint", nil)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp CheckpointResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.RecordProcessed, uint64(0))
	assert.GreaterOrEqual(t, resp.SegmentID, uint32(0))
	assert.GreaterOrEqual(t, resp.Offset, int64(0))
}

func TestBackupSegmentsAfter_Success(t *testing.T) {
	ts, cleanup := setupTestServerWithConfig(t, func(cfg *dbkernel.EngineConfig) {
		cfg.WalConfig.SegmentSize = 64 * 1024
	})
	defer cleanup()

	payload := bytes.Repeat([]byte("x"), 8*1024)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		require.NoError(t, ts.engine.PutKV([]byte(key), payload))
	}

	offset := ts.engine.CurrentOffset()
	require.NotNil(t, offset)
	require.Greater(t, offset.SegmentID, uint32(1), "expected WAL rotation to seal prior segments")

	backupDir := filepath.Join("wal", "case-a")
	reqBody := BackupSegmentsRequest{
		AfterSegmentID: 0,
		BackupDir:      backupDir,
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/wal/backup", reqBody)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BackupSegmentsResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Backups)

	for _, entry := range resp.Backups {
		assert.Greater(t, entry.SegmentID, uint32(0))
		assert.True(t, strings.HasPrefix(entry.Path, ts.backupRoot))
		_, statErr := os.Stat(entry.Path)
		assert.NoError(t, statErr)
	}
}

func TestBackupSegmentsAfter_NoSegments(t *testing.T) {
	ts, cleanup := setupTestServerWithConfig(t, func(cfg *dbkernel.EngineConfig) {
		cfg.WalConfig.SegmentSize = 64 * 1024
	})
	defer cleanup()

	backupDir := "wal/empty"
	reqBody := BackupSegmentsRequest{
		AfterSegmentID: 99,
		BackupDir:      backupDir,
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/wal/backup", reqBody)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestBackupSegmentsAfter_InvalidRequest(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := map[string]any{
		"afterSegmentId": 0,
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/wal/backup", reqBody)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestBtreeBackup_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	require.NoError(t, ts.engine.PutKV([]byte("backup-key"), []byte("backup-value")))

	backupPath := filepath.Join("snapshots", "btree.snapshot")
	reqBody := BtreeBackupRequest{Path: backupPath}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/btree/backup", reqBody)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BtreeBackupResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	expectedPath := filepath.Join(ts.backupRoot, backupPath)
	assert.Equal(t, expectedPath, resp.Path)
	assert.Greater(t, resp.Bytes, int64(0))

	info, err := os.Stat(expectedPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}

func TestBtreeBackup_InvalidRequest(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := BtreeBackupRequest{}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/btree/backup", reqBody)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestInvalidJSON(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/test/kv/testkey", bytes.NewReader([]byte("{invalid json")))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ts.router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "invalid request body")
}

func TestNamespaceNotFound(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/nonexistent/offset", nil)

	assert.Equal(t, http.StatusNotFound, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "namespace not found")
}

func BenchmarkPutKV(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := dbkernel.NewDefaultEngineConfig()
	cfg.DBEngine = dbkernel.LMDBEngine

	engine, err := dbkernel.NewStorageEngine(tmpDir, "test", cfg)
	require.NoError(b, err)
	defer engine.Close(context.Background())

	engines := map[string]*dbkernel.Engine{"test": engine}
	service := NewService(engines)
	router := mux.NewRouter()
	service.RegisterRoutes(router)

	reqBody := PutKVRequest{
		Value: encodeBase64([]byte("benchmark-value")),
	}
	jsonBody, _ := json.Marshal(reqBody)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPut, "/api/v1/test/kv/bench-key", bytes.NewReader(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkGetKV(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := dbkernel.NewDefaultEngineConfig()
	cfg.DBEngine = dbkernel.LMDBEngine

	engine, err := dbkernel.NewStorageEngine(tmpDir, "test", cfg)
	require.NoError(b, err)
	defer engine.Close(context.Background())

	err = engine.PutKV([]byte("bench-key"), []byte("bench-value"))
	require.NoError(b, err)

	engines := map[string]*dbkernel.Engine{"test": engine}
	service := NewService(engines)
	router := mux.NewRouter()
	service.RegisterRoutes(router)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/test/kv/bench-key", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkBatchPutKV(b *testing.B) {
	tmpDir := b.TempDir()
	cfg := dbkernel.NewDefaultEngineConfig()
	cfg.DBEngine = dbkernel.LMDBEngine

	engine, err := dbkernel.NewStorageEngine(tmpDir, "test", cfg)
	require.NoError(b, err)
	defer engine.Close(context.Background())

	engines := map[string]*dbkernel.Engine{"test": engine}
	service := NewService(engines)
	router := mux.NewRouter()
	service.RegisterRoutes(router)

	items := make([]BatchKVItem, 10)
	for i := 0; i < 10; i++ {
		items[i] = BatchKVItem{
			Key:   "key-" + string(rune('0'+i)),
			Value: encodeBase64([]byte("value-" + string(rune('0'+i)))),
		}
	}

	reqBody := BatchKVRequest{
		Operation: "put",
		Items:     items,
	}
	jsonBody, _ := json.Marshal(reqBody)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/test/kv/batch", bytes.NewReader(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func TestRequestSizeLimit_PutKV(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	largeData := make([]byte, 800*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	req := PutKVRequest{
		Value: base64.StdEncoding.EncodeToString(largeData),
	}

	reqBody, err := json.Marshal(req)
	require.NoError(t, err)

	require.Greater(t, len(reqBody), maxRequestBodySize)

	rr := httptest.NewRecorder()
	httpReq, err := http.NewRequest(http.MethodPut, "/api/v1/test/kv/large-key", bytes.NewReader(reqBody))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")

	ts.router.ServeHTTP(rr, httpReq)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "request body too large")
}

func TestRequestSizeLimit_BatchKV(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	var items []BatchKVItem
	for i := 0; i < 1100; i++ {
		data := make([]byte, 1024)
		for j := range data {
			data[j] = byte(j % 256)
		}
		items = append(items, BatchKVItem{
			Key:   "key" + string(rune(i)),
			Value: base64.StdEncoding.EncodeToString(data),
		})
	}

	req := BatchKVRequest{
		Operation: "put",
		Items:     items,
	}

	reqBody, err := json.Marshal(req)
	require.NoError(t, err)
	require.Greater(t, len(reqBody), maxRequestBodySize)

	rr := httptest.NewRecorder()
	httpReq, err := http.NewRequest(http.MethodPost, "/api/v1/test/kv/batch", bytes.NewReader(reqBody))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")

	ts.router.ServeHTTP(rr, httpReq)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "request body too large")
}

func TestRequestSizeLimit_Transaction_AppendLOB(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "lob",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	require.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	err := json.NewDecoder(rr.Body).Decode(&beginResp)
	require.NoError(t, err)
	txnID := beginResp.TxnID

	largeData := make([]byte, 2*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	rr = httptest.NewRecorder()
	httpReq, err := http.NewRequest(
		http.MethodPost,
		"/api/v1/test/tx/"+txnID+"/lob?key=large-file",
		bytes.NewReader(largeData),
	)
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/octet-stream")

	ts.router.ServeHTTP(rr, httpReq)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(t, rr.Body.String(), "failed to read request body")
}

func TestRequestSizeLimit_SmallRequest_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	smallData := []byte("test value")
	req := PutKVRequest{
		Value: base64.StdEncoding.EncodeToString(smallData),
	}

	reqBody, err := json.Marshal(req)
	require.NoError(t, err)
	require.Less(t, len(reqBody), maxRequestBodySize)

	rr := httptest.NewRecorder()
	httpReq, err := http.NewRequest(http.MethodPut, "/api/v1/test/kv/small-key", bytes.NewReader(reqBody))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")

	ts.router.ServeHTTP(rr, httpReq)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp PutKVResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)

	value, err := ts.engine.GetKV([]byte("small-key"))
	require.NoError(t, err)
	assert.Equal(t, smallData, value)
}

func TestRequestSizeLimit_AlmostMaxSize_Success(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	almostMaxData := make([]byte, 750*1024)
	for i := range almostMaxData {
		almostMaxData[i] = byte(i % 256)
	}

	req := PutKVRequest{
		Value: base64.StdEncoding.EncodeToString(almostMaxData),
	}

	reqBody, err := json.Marshal(req)
	require.NoError(t, err)

	t.Logf("Request body size: %d bytes (limit: %d)", len(reqBody), maxRequestBodySize)
	require.Less(t, len(reqBody), maxRequestBodySize)

	rr := httptest.NewRecorder()
	httpReq, err := http.NewRequest(http.MethodPut, "/api/v1/test/kv/almost-max-key", bytes.NewReader(reqBody))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")

	ts.router.ServeHTTP(rr, httpReq)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp PutKVResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestRequestSizeLimit_ErrorMessage(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	largeData := strings.Repeat("x", 2*1024*1024)
	req := PutKVRequest{
		Value: base64.StdEncoding.EncodeToString([]byte(largeData)),
	}

	reqBody, err := json.Marshal(req)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	httpReq, err := http.NewRequest(http.MethodPut, "/api/v1/test/kv/oversized", bytes.NewReader(reqBody))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")

	ts.router.ServeHTTP(rr, httpReq)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err = json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)

	assert.Contains(t, errResp["error"], "invalid request body")
	t.Logf("Error message: %s", errResp["error"])
}

func setupReadOnlyTestServer(t *testing.T) (*testServer, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	writeCfg := dbkernel.NewDefaultEngineConfig()
	writeCfg.DBEngine = dbkernel.LMDBEngine

	writeEngine, err := dbkernel.NewStorageEngine(tmpDir, "test", writeCfg)
	require.NoError(t, err)

	err = writeEngine.PutKV([]byte("existing-key"), []byte("existing-value"))
	require.NoError(t, err)

	columns := map[string][]byte{
		"col1": []byte("value1"),
		"col2": []byte("value2"),
	}
	err = writeEngine.PutColumnsForRow([]byte("existing-row"), columns)
	require.NoError(t, err)

	err = writeEngine.Close(context.Background())
	require.NoError(t, err)

	readOnlyCfg := dbkernel.NewDefaultEngineConfig()
	readOnlyCfg.DBEngine = dbkernel.LMDBEngine
	readOnlyCfg.ReadOnly = true

	engine, err := dbkernel.NewStorageEngine(tmpDir, "test", readOnlyCfg)
	require.NoError(t, err)

	engines := map[string]*dbkernel.Engine{
		"test": engine,
	}

	service := NewService(engines)
	router := mux.NewRouter()
	service.RegisterRoutes(router)

	cleanup := func() {
		engine.Close(context.Background())
	}

	return &testServer{
		service: service,
		router:  router,
		engine:  engine,
	}, cleanup
}

func TestReadOnlyMode_HTTPAPIPutKV(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	reqBody := PutKVRequest{
		Value: encodeBase64([]byte("new-value")),
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/kv/new-key", reqBody)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "read-only mode")
}

func TestReadOnlyMode_HTTPAPIBatchPutKV(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: "put",
		Items: []BatchKVItem{
			{Key: "key1", Value: encodeBase64([]byte("value1"))},
			{Key: "key2", Value: encodeBase64([]byte("value2"))},
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "read-only mode")
}

func TestReadOnlyMode_HTTPAPIDeleteKV(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	rr := makeRequest(t, ts.router, http.MethodDelete, "/api/v1/test/kv/existing-key", nil)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "read-only mode")
}

func TestReadOnlyMode_HTTPAPIBatchDeleteKV(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	reqBody := BatchKVRequest{
		Operation: "delete",
		Keys:      []string{"existing-key"},
	}

	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/kv/batch", reqBody)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "read-only mode")
}

func TestReadOnlyMode_HTTPAPIPutRow(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	reqBody := PutRowRequest{
		Columns: map[string]string{
			"col1": encodeBase64([]byte("value1")),
		},
	}

	rr := makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/row/new-row", reqBody)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "read-only mode")
}

func TestReadOnlyMode_HTTPAPIDeleteRow(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	rr := makeRequest(t, ts.router, http.MethodDelete, "/api/v1/test/row/existing-row", nil)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "read-only mode")
}

func TestReadOnlyMode_HTTPAPIDeleteColumns(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	reqBody := DeleteColumnsRequest{
		Columns: map[string]string{
			"col1": encodeBase64([]byte("value1")),
		},
	}

	rr := makeRequest(t, ts.router, http.MethodDelete, "/api/v1/test/row/existing-row/columns", reqBody)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "read-only mode")
}

func TestReadOnlyMode_HTTPAPIBatchRows(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	t.Run("put_operation", func(t *testing.T) {
		reqBody := BatchRowsRequest{
			Operation: "put",
			Rows: []BatchRowItem{
				{
					RowKey: "row1",
					Columns: map[string]string{
						"col1": encodeBase64([]byte("value1")),
					},
				},
			},
		}

		rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)

		var errResp map[string]string
		err := json.NewDecoder(rr.Body).Decode(&errResp)
		require.NoError(t, err)
		assert.Contains(t, errResp["error"], "read-only mode")
	})

	t.Run("delete_operation", func(t *testing.T) {
		reqBody := BatchRowsRequest{
			Operation: "delete",
			Rows: []BatchRowItem{
				{RowKey: "existing-row"},
			},
		}

		rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/row/batch", reqBody)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)

		var errResp map[string]string
		err := json.NewDecoder(rr.Body).Decode(&errResp)
		require.NoError(t, err)
		assert.Contains(t, errResp["error"], "read-only mode")
	})
}

func TestReadOnlyMode_HTTPAPIReadsAllowed(t *testing.T) {
	ts, cleanup := setupReadOnlyTestServer(t)
	defer cleanup()

	t.Run("get_kv", func(t *testing.T) {
		rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/kv/existing-key", nil)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp GetKVResponse
		err := json.NewDecoder(rr.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Found)
		assert.Equal(t, []byte("existing-value"), decodeBase64(t, resp.Value))
	})

	t.Run("get_row", func(t *testing.T) {
		rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/row/existing-row", nil)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp GetRowResponse
		err := json.NewDecoder(rr.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Found)
		assert.Len(t, resp.Columns, 2)
		assert.Equal(t, []byte("value1"), decodeBase64(t, resp.Columns["col1"]))
		assert.Equal(t, []byte("value2"), decodeBase64(t, resp.Columns["col2"]))
	})

	t.Run("get_offset", func(t *testing.T) {
		rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/offset", nil)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp OffsetResponse
		err := json.NewDecoder(rr.Body).Decode(&resp)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, resp.SegmentID, uint32(0))
		assert.GreaterOrEqual(t, resp.Offset, int64(0))
	})

	t.Run("get_stats", func(t *testing.T) {
		rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/stats", nil)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp StatsResponse
		err := json.NewDecoder(rr.Body).Decode(&resp)
		require.NoError(t, err)
		assert.Equal(t, "test", resp.Namespace)
		assert.GreaterOrEqual(t, resp.OpsReceived, uint64(0))
	})

	t.Run("get_checkpoint", func(t *testing.T) {
		rr := makeRequest(t, ts.router, http.MethodGet, "/api/v1/test/checkpoint", nil)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp CheckpointResponse
		err := json.NewDecoder(rr.Body).Decode(&resp)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, resp.SegmentID, uint32(0))
	})
}

func TestHealthEndpoint(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("health_check", func(t *testing.T) {
		rr := makeRequest(t, ts.router, http.MethodGet, "/health", nil)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp HealthResponse
		err := json.NewDecoder(rr.Body).Decode(&resp)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp.Status)
		assert.Contains(t, resp.Namespaces, "test")
		assert.Len(t, resp.Namespaces, 1)
	})
}
