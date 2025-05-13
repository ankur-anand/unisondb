package walfs

import (
	"sync"
	"testing"
)

func TestReaderTracker_AddAndContains(t *testing.T) {
	rt := newReaderTracker()

	id := uint64(42)
	if rt.Contains(id) {
		t.Fatalf("expected Contains(%d) to be false before Add", id)
	}

	rt.Add(id)
	if !rt.Contains(id) {
		t.Fatalf("expected Contains(%d) to be true after Add", id)
	}
}

func TestReaderTracker_Remove(t *testing.T) {
	rt := newReaderTracker()

	id := uint64(99)
	rt.Add(id)

	if !rt.Remove(id) {
		t.Fatalf("expected Remove(%d) to return true", id)
	}

	if rt.Contains(id) {
		t.Fatalf("expected Contains(%d) to be false after Remove", id)
	}

	if rt.Remove(id) {
		t.Fatalf("expected Remove(%d) to return false after already removed", id)
	}
}

func TestReaderTracker_HasAny(t *testing.T) {
	rt := newReaderTracker()

	if rt.HasAny() {
		t.Fatalf("expected HasAny() to be false on empty tracker")
	}

	rt.Add(1)
	if !rt.HasAny() {
		t.Fatalf("expected HasAny() to be true after Add")
	}

	rt.Remove(1)
	if rt.HasAny() {
		t.Fatalf("expected HasAny() to be false after Remove")
	}
}

func TestReaderTracker_ConcurrentAccess(t *testing.T) {
	rt := newReaderTracker()
	var wg sync.WaitGroup
	const total = 1000

	wg.Add(total)
	for i := 0; i < total; i++ {
		go func(id uint64) {
			defer wg.Done()
			rt.Add(id)
		}(uint64(i))
	}
	wg.Wait()

	if !rt.HasAny() {
		t.Fatalf("expected HasAny() to be true after concurrent adds")
	}

	wg.Add(total)
	for i := 0; i < total; i++ {
		go func(id uint64) {
			defer wg.Done()
			rt.Remove(id)
		}(uint64(i))
	}
	wg.Wait()

	if rt.HasAny() {
		t.Fatalf("expected HasAny() to be false after concurrent removes")
	}
}
