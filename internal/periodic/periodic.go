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

import "time"

// Periodic holds a background goroutine that can do periodic work.
//
// The work here cannot communicate errors directly, so it much
// communicate with channels or swallow errors.
type Periodic struct {
	duration time.Duration
	work     func()

	stop chan any
	done chan any
}

// New creates new background work that runs every `duration` and performs `work`.
func New(duration time.Duration, work func()) *Periodic {
	return &Periodic{
		duration: duration,
		work:     work,
	}
}

// Start starts the `Periodic` work.
//
// It does not do work immedately, only after the time has passed.
func (p *Periodic) Start() {
	if p.stop != nil {
		return
	}
	stop := make(chan any)
	done := make(chan any)
	go func() {
		refreshTimer := time.NewTicker(p.duration)
		defer func() {
			refreshTimer.Stop()
			close(done)
		}()
		for {
			select {
			case <-refreshTimer.C:
				p.work()
			case <-stop:
				return
			}
		}
	}()
	p.stop = stop
	p.done = done
}

// Stop stops the periodic work and waits for the background goroutine to exit.
func (p *Periodic) Stop() {
	if p.stop == nil {
		return
	}
	close(p.stop)
	<-p.done
	p.done = nil
	p.stop = nil
}
