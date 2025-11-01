package crdt

import (
	"encoding/json"
	"fmt"
)

// LWWRegister represents a Last-Write-Wins Register CRDT.
// It stores a value with a timestamp and resolves conflicts by choosing the latest timestamp.
// In case of tie, the replica with the higher ID wins.
type LWWRegister struct {
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	Timestamp int64       `json:"timestamp"`
	Replica   string      `json:"replica"`
}

// NewLWWRegister creates a new LWW-Register for the given key.
func NewLWWRegister(key string) *LWWRegister {
	return &LWWRegister{
		Key:       key,
		Value:     nil,
		Timestamp: 0,
		Replica:   "",
	}
}

// Update updates the register if the new timestamp is greater.
// Returns true if the register was updated, false if the update was stale.
func (r *LWWRegister) Update(value interface{}, timestamp int64, replica string) bool {
	if timestamp > r.Timestamp ||
		(timestamp == r.Timestamp && replica > r.Replica) {
		r.Value = value
		r.Timestamp = timestamp
		r.Replica = replica
		return true
	}
	return false
}

// GetValue returns the current value of the register.
func (r *LWWRegister) GetValue() interface{} {
	return r.Value
}

// GetState returns the current state of the register.
func (r *LWWRegister) GetState() map[string]interface{} {
	return map[string]interface{}{
		"key":       r.Key,
		"value":     r.Value,
		"timestamp": r.Timestamp,
		"replica":   r.Replica,
	}
}

// String returns a string representation of the register state.
func (r *LWWRegister) String() string {
	data, _ := json.MarshalIndent(r.GetState(), "", "  ")
	return string(data)
}

// LWWData represents the serialized format of LWW-Register data from UnisonDB.
type LWWData struct {
	Value     interface{} `json:"value"`
	Timestamp int64       `json:"timestamp"`
	Replica   string      `json:"replica"`
}

// Print displays the register state in a formatted way.
func (r *LWWRegister) Print() {
	fmt.Printf("  %s:\n", r.Key)
	fmt.Printf("    Value: %v\n", r.Value)
	fmt.Printf("    Timestamp: %d\n", r.Timestamp)
	fmt.Printf("    Replica: %s\n", r.Replica)
}
