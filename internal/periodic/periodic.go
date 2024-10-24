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

package periodic

import (
	"context"
	"time"
)

// Periodic holds a background goroutine that can do periodic work.
//
// The work here cannot communicate errors directly, so it must
// communicate with channels or swallow errors.
//
// NOTE: It's expected that Start and Stop are called on the same
// goroutine or be externally synchronized as to not race.
type Periodic struct {
	duration time.Duration
	work     func(context.Context)

	cancel context.CancelFunc
	done   chan any
}

// New creates new background work that runs every `duration` and performs `work`.
func New(duration time.Duration, work func()) *Periodic {
	return &Periodic{
		duration: duration,
		work:     func(context.Context) { work() },
	}
}

// NewWithContext creates new background work that runs every `duration` and performs `work`.
//
// Work is passed a context that is cancelled when the overall periodic is cancelled.
func NewWithContext(duration time.Duration, work func(context.Context)) *Periodic {
	return &Periodic{
		duration: duration,
		work:     work,
	}
}

// Start starts the `Periodic` work.
//
// It does not do work immedately, only after the time has passed.
func (p *Periodic) Start() {
	if p.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan any)
	go runBackgroundLoop(ctx, p.duration, done, p.work)
	p.cancel = cancel
	p.done = done
}

func runBackgroundLoop(ctx context.Context, d time.Duration, done chan any, work func(context.Context)) {
	refreshTimer := time.NewTicker(d)
	defer func() {
		refreshTimer.Stop()
		close(done)
	}()
	for ctx.Err() == nil {
		select {
		case <-refreshTimer.C:
			work(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// Stop stops the periodic work and waits for the background goroutine to exit.
func (p *Periodic) Stop() {
	if p.cancel == nil {
		return
	}
	p.cancel()
	<-p.done
	p.done = nil
	p.cancel = nil
}
