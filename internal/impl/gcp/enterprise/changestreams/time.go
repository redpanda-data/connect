// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import "time"

var now = time.Now

// timeCache is a cache for a single time value.
type timeCache struct {
	v time.Time     // cached value
	t time.Time     // when the value was cached
	d time.Duration // cache duration

	now func() time.Time
}

func (c *timeCache) get() time.Time {
	if c.v.IsZero() || c.now().Sub(c.t) > c.d {
		return time.Time{}
	}
	return c.v
}

func (c *timeCache) set(v time.Time) {
	c.v = v
	c.t = c.now()
}

// timeRange makes sure that we process records in monotonically increasing
// time order, and do not process records over a certain time range if the end
// time is set.
type timeRange struct {
	cur time.Time
	end time.Time
}

// tryClaim claims a time as part of the current time range if it is after the
// current start time and before the end time.
//
// If the time is claimed, the start time is updated to the claimed time.
//
// Returns true if the time is claimed, false otherwise.
func (r *timeRange) tryClaim(t time.Time) bool {
	if t.Before(r.cur) {
		return false
	}
	if !r.end.IsZero() && r.end.Compare(t) <= 0 {
		return false
	}

	r.cur = t
	return true
}

func (r *timeRange) now() time.Time {
	return r.cur
}
