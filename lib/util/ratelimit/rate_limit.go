// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ratelimit

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

//------------------------------------------------------------------------------

// Config is a config struct containing rate limit fields.
type Config struct {
	Count    int    `json:"count" yaml:"count"`
	Interval string `json:"interval" yaml:"interval"`
}

// NewConfig returns a rate limit configuration struct with default values.
func NewConfig() Config {
	return Config{
		Count:    1000,
		Interval: "1s",
	}
}

//------------------------------------------------------------------------------

// Type is a structure that tracks a rate limit, it can be shared across
// parallel processes in order to maintain a maximum rate of a protected
// resource.
type Type struct {
	mut         sync.Mutex
	bucket      int
	lastRefresh time.Time

	size   int
	period time.Duration
}

// New creates a rate limit from a configuration struct. This type is safe to
// share and call from parallel goroutines.
func New(conf Config) (*Type, error) {
	if conf.Count <= 0 {
		return nil, errors.New("count must be larger than zero")
	}
	period, err := time.ParseDuration(conf.Interval)
	if err != nil {
		return nil, fmt.Errorf("failed to parse interval: %v", err)
	}
	return &Type{
		bucket:      conf.Count,
		lastRefresh: time.Now(),
		size:        conf.Count,
		period:      period,
	}, nil
}

//------------------------------------------------------------------------------

// Get access to the rate limited resource. Returns a bool indicating whether
// access is permitted and, if false, an appropriate time period to wait before
// requesting again.
func (r *Type) Get() (bool, time.Duration) {
	r.mut.Lock()
	r.bucket--

	if r.bucket < 0 {
		r.bucket = 0
		remaining := r.period - time.Since(r.lastRefresh)

		if remaining > 0 {
			r.mut.Unlock()
			return false, remaining
		}
		r.bucket = r.size - 1
		r.lastRefresh = time.Now()
	}
	r.mut.Unlock()
	return true, 0
}

//------------------------------------------------------------------------------
