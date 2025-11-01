package crdt

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// StateManager manages multiple CRDT objects and handles updates from UnisonDB.
type StateManager struct {
	mu sync.RWMutex
	// key -> LWWRegister
	lwwRegisters map[string]*LWWRegister
	// key -> GCounter
	gCounters map[string]*GCounter
}

// NewStateManager creates a new CRDT state manager.
func NewStateManager() *StateManager {
	return &StateManager{
		lwwRegisters: make(map[string]*LWWRegister),
		gCounters:    make(map[string]*GCounter),
	}
}

// ProcessUpdate processes a key-value update from UnisonDB.
// Determines CRDT type based on key prefix and applies the update.
func (sm *StateManager) ProcessUpdate(keyBase64, valueBase64 string) error {
	keyBytes, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return fmt.Errorf("failed to decode key: %w", err)
	}
	key := string(keyBytes)

	valueBytes, err := base64.StdEncoding.DecodeString(valueBase64)
	if err != nil {
		return fmt.Errorf("failed to decode value: %w", err)
	}

	fmt.Printf("\nProcessing update: %s\n", key)

	if strings.HasPrefix(key, "lww:") {
		return sm.processLWWUpdate(key, valueBytes)
	} else if strings.HasPrefix(key, "counter:") {
		return sm.processGCounterUpdate(key, valueBytes)
	}

	fmt.Printf("Unknown CRDT type for key: %s\n", key)
	return nil
}

// processLWWUpdate processes an LWW-Register update.
func (sm *StateManager) processLWWUpdate(key string, valueBytes []byte) error {
	var data LWWData
	if err := json.Unmarshal(valueBytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal LWW data: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	register, exists := sm.lwwRegisters[key]
	if !exists {
		register = NewLWWRegister(key)
		sm.lwwRegisters[key] = register
	}

	updated := register.Update(data.Value, data.Timestamp, data.Replica)
	if updated {
		fmt.Printf("LWW-Register updated: %s\n", key)
		fmt.Printf("   Value: %v\n", register.GetValue())
		fmt.Printf("   Timestamp: %d\n", register.Timestamp)
		fmt.Printf("   Replica: %s\n", register.Replica)
	} else {
		fmt.Printf("LWW-Register ignored (stale): %s\n", key)
	}

	return nil
}

// processGCounterUpdate processes a G-Counter update.
func (sm *StateManager) processGCounterUpdate(key string, valueBytes []byte) error {
	var data GCounterData
	if err := json.Unmarshal(valueBytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal G-Counter data: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	counter, exists := sm.gCounters[key]
	if !exists {
		counter = NewGCounter(key)
		sm.gCounters[key] = counter
	}

	updated := counter.Merge(data.Replica, data.Count)
	if updated {
		fmt.Printf("G-Counter updated: %s\n", key)
		fmt.Printf("   Replica: %s, Count: %d\n", data.Replica, data.Count)
		fmt.Printf("   Total: %d\n", counter.GetValue())
	} else {
		fmt.Printf("G-Counter ignored (stale): %s\n", key)
	}

	return nil
}

// PrintState prints the current CRDT state.
func (sm *StateManager) PrintState() {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("CURRENT CRDT STATE")
	fmt.Println(strings.Repeat("=", 60))

	if len(sm.lwwRegisters) > 0 {
		fmt.Println("\nLWW-Registers:")
		for _, register := range sm.lwwRegisters {
			register.Print()
		}
	}

	if len(sm.gCounters) > 0 {
		fmt.Println("\nG-Counters:")
		for _, counter := range sm.gCounters {
			counter.Print()
		}
	}

	fmt.Println(strings.Repeat("=", 60) + "\n")
}

// GetLWWRegister returns the LWW-Register for the given key.
func (sm *StateManager) GetLWWRegister(key string) (*LWWRegister, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	register, exists := sm.lwwRegisters[key]
	return register, exists
}

// GetGCounter returns the G-Counter for the given key.
func (sm *StateManager) GetGCounter(key string) (*GCounter, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	counter, exists := sm.gCounters[key]
	return counter, exists
}
