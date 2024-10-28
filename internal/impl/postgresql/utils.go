package pgstream

import (
	"fmt"
	"strconv"
	"strings"
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

// LSNToInt64 converts a PostgreSQL LSN string to int64
func LSNToInt64(lsn string) (int64, error) {
	// Split the LSN into segments
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN format: %s", lsn)
	}

	// Parse both segments as hex with uint64 first
	upper, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse upper part: %w", err)
	}

	lower, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse lower part: %w", err)
	}

	// Combine the segments into a single int64
	// Upper part is shifted left by 32 bits
	result := int64((upper << 32) | lower)

	return result, nil
}

// Int64ToLSN converts an int64 to a PostgreSQL LSN string
func Int64ToLSN(value int64) string {
	// Convert to uint64 to handle the bitwise operations properly
	uvalue := uint64(value)

	// Extract upper and lower parts
	upper := uvalue >> 32
	lower := uvalue & 0xFFFFFFFF

	// Format as hexadecimal with proper padding
	return fmt.Sprintf("%X/%X", upper, lower)
}
