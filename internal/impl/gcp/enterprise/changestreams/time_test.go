// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeCache(t *testing.T) {
	t0 := time.Unix(0, 1000)

	var nowTime time.Time
	c := &timeCache{
		d: 2 * time.Second,
		now: func() time.Time {
			nowTime = nowTime.Add(time.Second)
			return nowTime
		},
	}

	// Empty cache
	assert.True(t, c.get().IsZero(), "expected zero time")

	// Set and get
	t.Log(nowTime)
	c.set(t0)
	assert.Equal(t, t0, c.get(), "time mismatch after set")

	// Get cached
	t.Log(nowTime)
	assert.Equal(t, t0, c.get(), "time mismatch from cache")

	// Cache expired
	t.Log(nowTime)
	assert.True(t, c.get().IsZero(), "expected zero time after expiration")
}

func TestTimeRange(t *testing.T) {
	r := timeRange{
		cur: time.Unix(0, 10_000),
		end: time.Unix(0, 20_000),
	}

	tests := []struct {
		time     time.Time
		expected bool
	}{
		{time.Unix(0, 10_000), true},
		{time.Unix(0, 10_000), true},
		{time.Unix(0, 11_000), true},
		{time.Unix(0, 11_000), true},
		{time.Unix(0, 19_000), true},
		{time.Unix(0, 20_000), false},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, r.tryClaim(test.time),
			"tryClaim(%v) returned unexpected result", test.time)
	}
}
