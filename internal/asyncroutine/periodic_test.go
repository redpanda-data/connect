// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asyncroutine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCancellation(t *testing.T) {
	counter := atomic.Int32{}
	p := NewPeriodic(time.Hour, func() {
		counter.Add(1)
	})
	p.Start()
	require.Equal(t, int32(0), counter.Load())
	p.Stop()
	require.Equal(t, int32(0), counter.Load())
}

func TestWorks(t *testing.T) {
	counter := atomic.Int32{}
	p := NewPeriodic(time.Millisecond, func() {
		counter.Add(1)
	})
	p.Start()
	require.Eventually(t, func() bool { return counter.Load() > 5 }, time.Second, time.Millisecond)
	p.Stop()
	snapshot := counter.Load()
	time.Sleep(time.Millisecond * 250)
	require.Equal(t, snapshot, counter.Load())
}

func TestWorksWithContext(t *testing.T) {
	active := atomic.Bool{}
	p := NewPeriodicWithContext(time.Millisecond, func(ctx context.Context) {
		active.Store(true)
		// Block until context is cancelled
		<-ctx.Done()
		active.Store(false)
	})
	p.Start()
	require.Eventually(t, func() bool { return active.Load() }, 10*time.Millisecond, time.Millisecond)
	p.Stop()
	require.False(t, active.Load())
}
