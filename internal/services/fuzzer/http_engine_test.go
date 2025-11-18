package fuzzer

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/ankur-anand/unisondb/dbkernel"
	"github.com/ankur-anand/unisondb/internal/services/httpapi"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestHTTPAPIEngineKVOperations(t *testing.T) {
	_, client := setupHTTPAPIEngine(t)

	key := []byte("k1")
	value := []byte("value")
	require.NoError(t, client.PutKV(key, value))

	got, err := client.GetKV(key)
	require.NoError(t, err)
	require.Equal(t, value, got)

	keys := [][]byte{[]byte("k2"), []byte("k3")}
	values := [][]byte{[]byte("v2"), []byte("v3")}
	require.NoError(t, client.BatchPutKV(keys, values))

	for i := range keys {
		got, err := client.GetKV(keys[i])
		require.NoError(t, err)
		require.Equal(t, values[i], got)
	}

	require.NoError(t, client.DeleteKV(key))
	_, err = client.GetKV(key)
	require.ErrorIs(t, err, dbkernel.ErrKeyNotFound)

	require.NoError(t, client.BatchDeleteKV(keys))
	for _, k := range keys {
		_, err := client.GetKV(k)
		require.ErrorIs(t, err, dbkernel.ErrKeyNotFound)
	}

	// ensure missing get still maps to ErrKeyNotFound
	_, err = client.GetKV([]byte("missing"))
	require.ErrorIs(t, err, dbkernel.ErrKeyNotFound)

}

func TestHTTPAPIEngineRowOperations(t *testing.T) {
	_, client := setupHTTPAPIEngine(t)

	rowKey := []byte("row-1")
	columns := map[string][]byte{
		"colA": []byte("value-a"),
		"colB": []byte("value-b"),
	}
	require.NoError(t, client.PutColumnsForRow(rowKey, columns))

	resp, err := client.GetRowColumns(string(rowKey), nil)
	require.NoError(t, err)
	require.Equal(t, columns, resp)

	filtered, err := client.GetRowColumns(string(rowKey), func(col string) bool {
		return col == "colB"
	})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"colB": columns["colB"]}, filtered)

	deleteCols := map[string][]byte{"colA": columns["colA"]}
	require.NoError(t, client.DeleteColumnsForRow(rowKey, deleteCols))

	remaining, err := client.GetRowColumns(string(rowKey), nil)
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"colB": columns["colB"]}, remaining)

}

func setupHTTPAPIEngine(t *testing.T) (*dbkernel.Engine, *HTTPAPIEngine) {
	t.Helper()
	baseDir := t.TempDir()
	namespace := "fuzzhttp"
	engine, err := dbkernel.NewStorageEngine(baseDir, namespace, dbkernel.NewDefaultEngineConfig())
	require.NoError(t, err)

	router := mux.NewRouter()
	service := httpapi.NewService(map[string]*dbkernel.Engine{namespace: engine})
	service.RegisterRoutes(router)
	server := httptest.NewServer(router)
	t.Cleanup(func() {
		server.Close()
		_ = engine.Close(context.Background())
	})

	client := NewHTTPAPIEngine(context.Background(), server.URL+"/api/v1", namespace, server.Client())
	return engine, client
}
