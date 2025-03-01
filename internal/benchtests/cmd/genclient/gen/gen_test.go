package gen

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockSetter struct {
	count atomic.Uint64
}

func (m *mockSetter) Set() {
	m.count.Add(1)
}

type mockGetter struct {
	count atomic.Uint64
}

func (m *mockGetter) Get() {
	m.count.Add(1)
}

// Test for RatioRobin
func TestRatioRobin(t *testing.T) {
	setOps := uint64(3) // 3 set
	getOps := uint64(2) // 2 get

	setter := &mockSetter{}
	getter := &mockGetter{}
	ratioRobin := NewRatioRobin(setter, getter, setOps, getOps)

	totalCalls := uint64(15)
	for i := uint64(0); i < totalCalls; i++ {
		ratioRobin.Gen()
	}

	expectedSetCalls := (totalCalls * setOps) / (setOps + getOps)
	expectedGetCalls := (totalCalls * getOps) / (setOps + getOps)

	assert.Equal(t, expectedSetCalls, setter.count.Load(), "set called not matched")
	assert.Equal(t, expectedGetCalls, getter.count.Load(), "get called not matched")
}
