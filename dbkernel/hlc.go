package dbkernel

import (
	"time"
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

// DecodeHLC Extract lastTime and counter.
func DecodeHLC(encodedHLC uint64) (uint64, uint16) {
	lastTime := encodedHLC >> 16           // Extract upper 48 bits
	counter := uint16(encodedHLC & 0xFFFF) // Extract lower 16 bits
	return lastTime, counter
}

// IsConcurrent return true if the two hybrid logical clock have happened at the same time
// but are distinct events.
func IsConcurrent(hlc1, hlc2 uint64) bool {
	lastTime1, counter1 := DecodeHLC(hlc1)
	lastTime2, counter2 := DecodeHLC(hlc2)
	return lastTime1 == lastTime2 && counter1 != counter2
}

// HLCNow return an encoded hybrid logical lock.
// 48 bits are current timestamp
// 16 bits are counter value. can track 65,536 (2^16) unique events per timestamp(nanosecond).
func HLCNow(counter uint64) uint64 {
	now := time.Now()
	adjustment := monotonic - now.Sub(startTime)

	// prevents backward time shifts
	if adjustment < 0 {
		adjustment = 0
	}

	ct := uint64(time.Now().Add(adjustment).UnixNano())
	// (physical time << 16) | counter
	return (ct << 16) | counter
}
