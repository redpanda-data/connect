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
	if v := c.get(); !v.IsZero() {
		t.Errorf("expected zero time, got %v", v)
	}

	// Set and get
	t.Log(nowTime)
	c.set(t0)
	if v := c.get(); !v.Equal(t0) {
		t.Errorf("expected %v, got %v", t0, v)
	}

	// Get cached
	t.Log(nowTime)
	if v := c.get(); !v.Equal(t0) {
		t.Errorf("expected %v, got %v", t0, v)
	}

	// Cache expired
	t.Log(nowTime)
	if v := c.get(); !v.IsZero() {
		t.Errorf("expected zero time, got %v", v)
	}
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
		if r.tryClaim(test.time) != test.expected {
			t.Errorf("Expected tryClaim(%v) to be %v", test.time, test.expected)
		}
	}
}
