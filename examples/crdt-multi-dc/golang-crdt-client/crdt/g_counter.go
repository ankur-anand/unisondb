package crdt

import (
	"encoding/json"
	"fmt"
)

// GCounter represents a Grow-Only Counter CRDT.
// It maintains per-replica counters and merges by taking the max of each replica's count.
type GCounter struct {
	Key string `json:"key"`
	// replica_id -> count
	Counts map[string]int `json:"counts"`
}

// NewGCounter creates a new G-Counter for the given key.
func NewGCounter(key string) *GCounter {
	return &GCounter{
		Key:    key,
		Counts: make(map[string]int),
	}
}

// Increment increments the counter for a specific replica.
func (c *GCounter) Increment(replica string, amount int) {
	current := c.Counts[replica]
	c.Counts[replica] = current + amount
}

// Merge merges updates from another replica.
// Returns true if the counter was updated, false if the update was stale.
func (c *GCounter) Merge(replica string, count int) bool {
	current, exists := c.Counts[replica]
	if !exists || count > current {
		c.Counts[replica] = count
		return true
	}
	return false
}

// GetValue returns the total count across all replicas.
func (c *GCounter) GetValue() int {
	total := 0
	for _, count := range c.Counts {
		total += count
	}
	return total
}

// GetState returns the current state of the counter.
func (c *GCounter) GetState() map[string]interface{} {
	return map[string]interface{}{
		"key":    c.Key,
		"total":  c.GetValue(),
		"counts": c.Counts,
	}
}

// String returns a string representation of the counter state.
func (c *GCounter) String() string {
	data, _ := json.MarshalIndent(c.GetState(), "", "  ")
	return string(data)
}

// GCounterData represents the serialized format of G-Counter data from UnisonDB.
type GCounterData struct {
	Replica string `json:"replica"`
	Count   int    `json:"count"`
}

// Print displays the counter state in a formatted way.
func (c *GCounter) Print() {
	fmt.Printf("  %s:\n", c.Key)
	fmt.Printf("    Total: %d\n", c.GetValue())
	countsJSON, _ := json.Marshal(c.Counts)
	fmt.Printf("    Per-replica: %s\n", string(countsJSON))
}
