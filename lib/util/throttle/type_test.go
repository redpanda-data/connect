package throttle

import (
	"testing"
	"time"
)

func TestBasicThrottle(t *testing.T) {
	closeChan := make(chan struct{})
	close(closeChan)

	throt := New(
		OptMaxUnthrottledRetries(3),
		OptThrottlePeriod(time.Second),
		OptCloseChan(closeChan),
	)

	for i := 0; i < 3; i++ {
		if !throt.Retry() {
			t.Errorf("Throttle blocked early: %v", i)
		}
	}

	if throt.Retry() {
		t.Errorf("Throttle didn't throttle at end")
	}
}

func TestThrottleReset(t *testing.T) {
	closeChan := make(chan struct{})
	close(closeChan)

	throt := New(
		OptMaxUnthrottledRetries(3),
		OptThrottlePeriod(time.Second),
		OptCloseChan(closeChan),
	)

	for j := 0; j < 3; j++ {
		for i := 0; i < 3; i++ {
			if !throt.Retry() {
				t.Errorf("Throttle blocked early: %v", i)
			}
		}
		if throt.Retry() {
			t.Errorf("Throttle didn't throttle at end")
		}
		throt.Reset()
	}
}
