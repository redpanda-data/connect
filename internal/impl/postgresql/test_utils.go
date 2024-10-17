package pgstream

import (
	"sync"
	"sync/atomic"
	"time"
)

// RateCounter is used to measure the rate of invocations
type RateCounter struct {
	count       int64
	lastChecked time.Time
	mutex       sync.Mutex
}

// NewRateCounter creates a new RateCounter
func NewRateCounter() *RateCounter {
	return &RateCounter{
		lastChecked: time.Now(),
	}
}

// Increment increases the counter by 1
func (rc *RateCounter) Increment() {
	atomic.AddInt64(&rc.count, 1)
}

// Rate calculates the current rate of invocations per second
func (rc *RateCounter) Rate() float64 {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	now := time.Now()
	duration := now.Sub(rc.lastChecked).Seconds()
	count := atomic.SwapInt64(&rc.count, 0)
	rc.lastChecked = now

	return float64(count) / duration
}
