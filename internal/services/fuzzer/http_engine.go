package fuzzer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ankur-anand/unisondb/dbkernel"
)

// HTTPAPIEngine implements the Engine interface on top of the httpapi.Service.
//
// apiBaseURL should point to the service prefix (e.g. http://localhost:8080/api/v1).
// Each method encodes/decodes payloads so the fuzzer can stress the full HTTP stack.
type HTTPAPIEngine struct {
	ctx       context.Context
	client    *http.Client
	basePath  string
	namespace string
}

const (
	httpOperationPut    = "put"
	httpOperationDelete = "delete"
)

// NewHTTPAPIEngine returns an Engine implementation backed by HTTP APIs.
// The provided client is optional; when nil a default client with a 10s timeout is used.
func NewHTTPAPIEngine(ctx context.Context, apiBaseURL, namespace string, client *http.Client) *HTTPAPIEngine {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPAPIEngine{
		ctx:       ctx,
		client:    client,
		basePath:  strings.TrimSuffix(apiBaseURL, "/"),
		namespace: namespace,
	}
}

// FuzzHTTPAPIOps runs the fuzzer loop against the HTTP API surface for the namespace.
func FuzzHTTPAPIOps(ctx context.Context, apiBaseURL, namespace string, opsPerSec, numWorkers int, stats *FuzzStats, includeReads bool) {
	hEngine := NewHTTPAPIEngine(ctx, apiBaseURL, namespace, nil)
	FuzzEngineOps(ctx, hEngine, opsPerSec, numWorkers, stats, namespace, includeReads)
}

func (h *HTTPAPIEngine) PutKV(key, value []byte) error {
	req := putKVPayload{Value: base64.StdEncoding.EncodeToString(value)}
	return h.doJSON(http.MethodPut, "/kv/"+url.PathEscape(string(key)), req, nil)
}

func (h *HTTPAPIEngine) BatchPutKV(keys, values [][]byte) error {
	items := make([]batchKVItem, len(keys))
	for i := range keys {
		items[i] = batchKVItem{
			Key:   string(keys[i]),
			Value: base64.StdEncoding.EncodeToString(values[i]),
		}
	}
	req := batchKVPayload{Operation: httpOperationPut, Items: items}
	return h.doJSON(http.MethodPost, "/kv/batch", req, nil)
}

func (h *HTTPAPIEngine) DeleteKV(key []byte) error {
	return h.doJSON(http.MethodDelete, "/kv/"+url.PathEscape(string(key)), nil, nil)
}

func (h *HTTPAPIEngine) BatchDeleteKV(keys [][]byte) error {
	out := make([]string, len(keys))
	for i := range keys {
		out[i] = string(keys[i])
	}
	req := batchKVPayload{Operation: httpOperationDelete, Keys: out}
	return h.doJSON(http.MethodPost, "/kv/batch", req, nil)
}

func (h *HTTPAPIEngine) PutColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error {
	cols := make(map[string]string, len(columnEntries))
	for name, value := range columnEntries {
		cols[name] = base64.StdEncoding.EncodeToString(value)
	}
	return h.doJSON(http.MethodPut, "/row/"+url.PathEscape(string(rowKey)), putRowPayload{Columns: cols}, nil)
}

func (h *HTTPAPIEngine) DeleteColumnsForRow(rowKey []byte, columnEntries map[string][]byte) error {
	cols := make(map[string]string, len(columnEntries))
	for name, value := range columnEntries {
		cols[name] = base64.StdEncoding.EncodeToString(value)
	}
	return h.doJSON(http.MethodDelete, "/row/"+url.PathEscape(string(rowKey))+"/columns", deleteColumnsPayload{Columns: cols}, nil)
}

func (h *HTTPAPIEngine) GetKV(key []byte) ([]byte, error) {
	var resp getKVResponse
	if err := h.doJSON(http.MethodGet, "/kv/"+url.PathEscape(string(key)), nil, &resp); err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, dbkernel.ErrKeyNotFound
	}
	value, err := base64.StdEncoding.DecodeString(resp.Value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (h *HTTPAPIEngine) GetRowColumns(rowKey string, predicate func(columnKey string) bool) (map[string][]byte, error) {
	var resp getRowResponse
	if err := h.doJSON(http.MethodGet, "/row/"+url.PathEscape(rowKey), nil, &resp); err != nil {
		return nil, err
	}
	if !resp.Found {
		return nil, dbkernel.ErrKeyNotFound
	}
	if predicate == nil {
		predicate = func(string) bool { return true }
	}
	out := make(map[string][]byte)
	for name, encoded := range resp.Columns {
		if !predicate(name) {
			continue
		}
		value, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode column %s: %w", name, err)
		}
		out[name] = value
	}
	return out, nil
}

func (h *HTTPAPIEngine) doJSON(method, path string, reqBody any, respBody any) error {
	var reader io.Reader
	if reqBody != nil {
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
		reader = buf
	}
	req, err := http.NewRequestWithContext(h.ctx, method, h.endpoint(path), reader)
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("http api %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if respBody == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("decode response %s %s: %w", method, path, err)
	}
	return nil
}

func (h *HTTPAPIEngine) endpoint(path string) string {
	return fmt.Sprintf("%s/%s%s", h.basePath, url.PathEscape(h.namespace), path)
}

type putKVPayload struct {
	Value string `json:"value"`
}

type batchKVItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type batchKVPayload struct {
	Operation string        `json:"operation"`
	Items     []batchKVItem `json:"items,omitempty"`
	Keys      []string      `json:"keys,omitempty"`
}

type putRowPayload struct {
	Columns map[string]string `json:"columns"`
}

type deleteColumnsPayload struct {
	Columns map[string]string `json:"columns"`
}

type getKVResponse struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type getRowResponse struct {
	Columns map[string]string `json:"columns"`
	Found   bool              `json:"found"`
}
