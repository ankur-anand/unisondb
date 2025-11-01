package client

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/ankur-anand/unisondb/examples/golang-crdt-client/crdt"
)

// HTTPClient provides access to the UnisonDB HTTP API.
type HTTPClient struct {
	baseURL      string
	httpClient   *http.Client
	stateManager *crdt.StateManager
}

// NewHTTPClient creates a new HTTP client for UnisonDB.
func NewHTTPClient(baseURL string, stateManager *crdt.StateManager) *HTTPClient {
	return &HTTPClient{
		baseURL:      baseURL,
		httpClient:   &http.Client{},
		stateManager: stateManager,
	}
}

// KVGetResponse represents the response from a GET /kv/{key} request.
type KVGetResponse struct {
	// base64-encoded value
	Value string `json:"value"`
	Found bool   `json:"found"`
}

// FetchValue fetches the current value for a key from the specified namespace.
// It then processes the update through the state manager.
func (c *HTTPClient) FetchValue(key, namespace string) error {

	encodedKey := url.PathEscape(key)
	reqURL := fmt.Sprintf("%s/%s/kv/%s", c.baseURL, namespace, encodedKey)

	resp, err := c.httpClient.Get(reqURL)
	if err != nil {
		return fmt.Errorf("failed to fetch value: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var kvResp KVGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&kvResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if kvResp.Found {
		// Base64 encode the key for the state manager
		keyBase64 := base64.StdEncoding.EncodeToString([]byte(key))
		return c.stateManager.ProcessUpdate(keyBase64, kvResp.Value)
	}

	return nil
}

// FetchMultipleValues fetches values for multiple keys from the specified namespace.
func (c *HTTPClient) FetchMultipleValues(keys []string, namespace string) error {
	for _, key := range keys {
		if err := c.FetchValue(key, namespace); err != nil {
			fmt.Printf("Error fetching %s from %s: %v\n", key, namespace, err)
		}
	}
	return nil
}
