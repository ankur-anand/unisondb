package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionLifecycle_KV(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "kv",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	err := json.NewDecoder(rr.Body).Decode(&beginResp)
	require.NoError(t, err)
	assert.True(t, beginResp.Success)
	assert.NotEmpty(t, beginResp.TxnID)
	txnID := beginResp.TxnID

	kvPairs := []struct {
		key   string
		value string
	}{
		{"txkey1", "txvalue1"},
		{"txkey2", "txvalue2"},
		{"txkey3", "txvalue3"},
	}

	for _, kv := range kvPairs {
		appendReq := AppendKVRequest{
			Key:   kv.key,
			Value: encodeBase64([]byte(kv.value)),
		}
		rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/kv", appendReq)
		assert.Equal(t, http.StatusOK, rr.Code)
	}

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/commit", nil)
	assert.Equal(t, http.StatusOK, rr.Code)

	var commitResp CommitTransactionResponse
	err = json.NewDecoder(rr.Body).Decode(&commitResp)
	require.NoError(t, err)
	assert.True(t, commitResp.Success)

	for _, kv := range kvPairs {
		value, err := ts.engine.GetKV([]byte(kv.key))
		require.NoError(t, err)
		assert.Equal(t, []byte(kv.value), value)
	}
}

func TestTransactionLifecycle_Row(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "row",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	err := json.NewDecoder(rr.Body).Decode(&beginResp)
	require.NoError(t, err)
	assert.True(t, beginResp.Success)
	txnID := beginResp.TxnID

	rows := []struct {
		rowKey  string
		columns map[string]string
	}{
		{
			rowKey: "row1",
			columns: map[string]string{
				"col1": encodeBase64([]byte("val1")),
				"col2": encodeBase64([]byte("val2")),
			},
		},
		{
			rowKey: "row2",
			columns: map[string]string{
				"colA": encodeBase64([]byte("valA")),
				"colB": encodeBase64([]byte("valB")),
			},
		},
	}

	for _, row := range rows {
		appendReq := AppendRowRequest{
			RowKey:  row.rowKey,
			Columns: row.columns,
		}
		rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/row", appendReq)
		assert.Equal(t, http.StatusOK, rr.Code)
	}

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/commit", nil)
	assert.Equal(t, http.StatusOK, rr.Code)

	var commitResp CommitTransactionResponse
	err = json.NewDecoder(rr.Body).Decode(&commitResp)
	require.NoError(t, err)
	assert.True(t, commitResp.Success)

	row1Cols, err := ts.engine.GetRowColumns("row1", func(string) bool { return true })
	require.NoError(t, err)
	assert.Len(t, row1Cols, 2)
	assert.Equal(t, []byte("val1"), row1Cols["col1"])
	assert.Equal(t, []byte("val2"), row1Cols["col2"])

	row2Cols, err := ts.engine.GetRowColumns("row2", func(string) bool { return true })
	require.NoError(t, err)
	assert.Len(t, row2Cols, 2)
	assert.Equal(t, []byte("valA"), row2Cols["colA"])
	assert.Equal(t, []byte("valB"), row2Cols["colB"])
}

func TestTransactionCommitConflictWithNonTxnKV(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{Operation: "put", EntryType: "kv"}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&beginResp))
	txnID := beginResp.TxnID

	appendReq := AppendKVRequest{Key: "kv-conflict", Value: encodeBase64([]byte("old"))}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/kv", appendReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	// concurrent non-txn write
	putReq := PutKVRequest{Value: encodeBase64([]byte("newer"))}
	rr = makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/kv/kv-conflict", putReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/commit", nil)
	assert.Equal(t, http.StatusConflict, rr.Code)
	var errResp map[string]string
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&errResp))
	assert.Contains(t, errResp["error"], "txn conflict")

	val, err := ts.engine.GetKV([]byte("kv-conflict"))
	require.NoError(t, err)
	assert.Equal(t, []byte("newer"), val)
}

func TestTransactionCommitConflictWithNonTxnRow(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{Operation: "put", EntryType: "row"}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&beginResp))
	txnID := beginResp.TxnID

	appendReq := AppendRowRequest{
		RowKey: "row-conflict",
		Columns: map[string]string{
			"c1": encodeBase64([]byte("old")),
		},
	}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/row", appendReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	// concurrent non-txn row write
	putRowReq := PutRowRequest{
		Columns: map[string]string{
			"c1": encodeBase64([]byte("newer")),
		},
	}
	rr = makeRequest(t, ts.router, http.MethodPut, "/api/v1/test/row/row-conflict", putRowReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/commit", nil)
	assert.Equal(t, http.StatusConflict, rr.Code)
	var errResp map[string]string
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&errResp))
	assert.Contains(t, errResp["error"], "txn conflict")

	rowCols, err := ts.engine.GetRowColumns("row-conflict", nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("newer"), rowCols["c1"])
}

func TestTransactionLifecycle_LOB(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "lob",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	err := json.NewDecoder(rr.Body).Decode(&beginResp)
	require.NoError(t, err)
	assert.True(t, beginResp.Success)
	txnID := beginResp.TxnID

	largeData := make([]byte, 100*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	chunkSize := 32 * 1024
	for i := 0; i < len(largeData); i += chunkSize {
		end := i + chunkSize
		if end > len(largeData) {
			end = len(largeData)
		}
		chunk := largeData[i:end]

		req := newRequestWithBody(t, http.MethodPost, "/api/v1/test/tx/"+txnID+"/lob?key=large-key", bytes.NewReader(chunk))
		rr := executeRequest(t, ts.router, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	}

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/commit", nil)
	assert.Equal(t, http.StatusOK, rr.Code)

	var commitResp CommitTransactionResponse
	err = json.NewDecoder(rr.Body).Decode(&commitResp)
	require.NoError(t, err)
	assert.True(t, commitResp.Success)
	assert.NotZero(t, commitResp.Checksum)

	value, err := ts.engine.GetLOB([]byte("large-key"))
	require.NoError(t, err)
	assert.Equal(t, largeData, value)
}

func TestParallelTransactionsConflictOnlyOneSucceeds(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{Operation: "put", EntryType: "kv"}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	require.Equal(t, http.StatusOK, rr.Code)
	var beginResp1 BeginTransactionResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&beginResp1))

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	require.Equal(t, http.StatusOK, rr.Code)
	var beginResp2 BeginTransactionResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&beginResp2))

	appendReq := AppendKVRequest{Key: "race-key", Value: encodeBase64([]byte("txn1"))}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+beginResp1.TxnID+"/kv", appendReq)
	require.Equal(t, http.StatusOK, rr.Code)

	appendReq = AppendKVRequest{Key: "race-key", Value: encodeBase64([]byte("txn2"))}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+beginResp2.TxnID+"/kv", appendReq)
	require.Equal(t, http.StatusOK, rr.Code)

	rr2 := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+beginResp2.TxnID+"/commit", nil)
	require.Equal(t, http.StatusOK, rr2.Code)

	rr1 := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+beginResp1.TxnID+"/commit", nil)
	require.Equal(t, http.StatusConflict, rr1.Code)

	val, err := ts.engine.GetKV([]byte("race-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("txn2"), val)
}

func TestTransactionAbort(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "kv",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	err := json.NewDecoder(rr.Body).Decode(&beginResp)
	require.NoError(t, err)
	txnID := beginResp.TxnID

	appendReq := AppendKVRequest{
		Key:   "abort-key",
		Value: encodeBase64([]byte("abort-value")),
	}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/kv", appendReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/abort", nil)
	assert.Equal(t, http.StatusOK, rr.Code)

	_, err = ts.engine.GetKV([]byte("abort-key"))
	assert.ErrorIs(t, err, dbkernel.ErrKeyNotFound)

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/commit", nil)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestTransactionNotFound(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	fakeTxnID := "nonexistenttxnid1234567890"

	appendReq := AppendKVRequest{
		Key:   "key",
		Value: encodeBase64([]byte("value")),
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+fakeTxnID+"/kv", appendReq)
	assert.Equal(t, http.StatusNotFound, rr.Code)

	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+fakeTxnID+"/commit", nil)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestTransactionInvalidOperation(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "invalid",
		EntryType: "kv",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestBeginTransaction_InvalidJSON(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/test/tx/begin", bytes.NewBufferString("{invalid json"))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	ts.router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp map[string]string
	err := json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "invalid request body")
}

func TestTransactionInvalidEntryType(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "invalid",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestTransactionEmptyKey(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "kv",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	json.NewDecoder(rr.Body).Decode(&beginResp)
	txnID := beginResp.TxnID

	appendReq := AppendKVRequest{
		Key:   "",
		Value: encodeBase64([]byte("value")),
	}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/kv", appendReq)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestTransactionInvalidBase64(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "kv",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	json.NewDecoder(rr.Body).Decode(&beginResp)
	txnID := beginResp.TxnID

	appendReq := AppendKVRequest{
		Key:   "key",
		Value: "not-valid-base64!!!",
	}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/kv", appendReq)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestTransactionRowEmptyColumns(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "row",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	json.NewDecoder(rr.Body).Decode(&beginResp)
	txnID := beginResp.TxnID

	appendReq := AppendRowRequest{
		RowKey:  "row1",
		Columns: map[string]string{},
	}
	rr = makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/"+txnID+"/row", appendReq)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestTransactionLOBMissingKey(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "lob",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	json.NewDecoder(rr.Body).Decode(&beginResp)
	txnID := beginResp.TxnID

	req := newRequestWithBody(t, http.MethodPost, "/api/v1/test/tx/"+txnID+"/lob", bytes.NewReader([]byte("data")))
	rr = executeRequest(t, ts.router, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestTransactionAppendLOBReadError(t *testing.T) {
	ts, cleanup := setupTestServer(t)
	defer cleanup()

	beginReq := BeginTransactionRequest{
		Operation: "put",
		EntryType: "lob",
	}
	rr := makeRequest(t, ts.router, http.MethodPost, "/api/v1/test/tx/begin", beginReq)
	assert.Equal(t, http.StatusOK, rr.Code)

	var beginResp BeginTransactionResponse
	err := json.NewDecoder(rr.Body).Decode(&beginResp)
	require.NoError(t, err)
	txnID := beginResp.TxnID

	req := httptest.NewRequest(http.MethodPost, "/api/v1/test/tx/"+txnID+"/lob?key=lob-read-error", errorReader{})
	rr = httptest.NewRecorder()
	ts.router.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var errResp map[string]string
	err = json.NewDecoder(rr.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Contains(t, errResp["error"], "failed to read request body")
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("read failure")
}

func newRequestWithBody(t *testing.T, method, url string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	require.NoError(t, err)
	return req
}

func executeRequest(t *testing.T, router *mux.Router, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}
