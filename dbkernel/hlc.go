package dbkernel

import (
	"time"
)

const (
	logicalBits = 23
	logicalMask = (1 << logicalBits) - 1
	timeMask    = (1 << (64 - logicalBits)) - 1
	// CustomEpochMs is a Sentinel Value for Jan 1, 2025 @ 00:00:00 UTC.
	CustomEpochMs = 1735689600000
)

// wall clock can jump forward or backward by the ntp.
// monotonic time don't.
// the process get monotonic time at the start of the process, so during it's life-time
// https://github.com/golang/go/blob/889abb17e125bb0f5d8de61bb80ef15fbe2a130d/src/runtime/time_nofake.go#L19
var startTime = time.Now()

var monotonic time.Duration

func initMonotonic() {
	monotonic = time.Since(startTime)
}

// HLCDecode Extract lastTime and counter from the encoded HLC.
func HLCDecode(encodedHLC uint64) (uint64, uint32) {
	// Extract upper 41 bits
	lastTime := encodedHLC >> logicalBits
	counter := uint32(encodedHLC & logicalMask)
	return lastTime, counter
}

// IsConcurrent return true if the two hybrid logical clock have happened at the same time
// but are distinct events within a millisecond interval.
func IsConcurrent(hlc1, hlc2 uint64) bool {
	lastTime1, counter1 := HLCDecode(hlc1)
	lastTime2, counter2 := HLCDecode(hlc2)
	return lastTime1 == lastTime2 && counter1 != counter2
}

var lastHLC uint64

// HLCNow returns an encoded hybrid logical clock. 41 bits = ms timestamp since custom epoch and 23 bits = logical counter.
func HLCNow() uint64 {
	now := time.Now()
	adjustment := monotonic - now.Sub(startTime)
	if adjustment < 0 {
		adjustment = 0
	}

	ms := uint64(now.Add(adjustment).UnixMilli()) - CustomEpochMs
	ms &= timeMask

	prev := lastHLC
	prevTS, prevCounter := HLCDecode(prev)

	var newTS uint64
	var newCounter uint64

	switch {
	case ms > prevTS:
		newTS = ms
		newCounter = 0
	case ms == prevTS:
		newTS = ms
		newCounter = uint64(prevCounter) + 1
		if newCounter > logicalMask {
			panic("HLC counter overflow: too many events in one ms")
		}
	case ms < prevTS:
		newTS = prevTS
		newCounter = uint64(prevCounter) + 1
		if newCounter > logicalMask {
			panic("HLC counter overflow: time regression + counter overflow")
		}
	}

	hlc := (newTS << logicalBits) | (newCounter & logicalMask)
	lastHLC = hlc
	return hlc
}
