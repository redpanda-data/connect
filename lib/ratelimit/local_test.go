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
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func TestLocalRateLimitConfErrors(t *testing.T) {
	conf := NewConfig()
	conf.Local.Count = -1
	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("expected error from bad count")
	}

	conf = NewConfig()
	conf.Local.Interval = "nope"
	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("expected error from bad interval")
	}
}

func TestLocalRateLimitBasic(t *testing.T) {
	conf := NewConfig()
	conf.Local.Count = 10
	conf.Local.Interval = "1s"

	rl, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < conf.Local.Count; i++ {
		period, _ := rl.Access()
		if period > 0 {
			t.Errorf("Period above zero: %v", period)
		}
	}

	if period, _ := rl.Access(); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

func TestLocalRateLimitRefresh(t *testing.T) {
	conf := NewConfig()
	conf.Local.Count = 10
	conf.Local.Interval = "10ms"

	rl, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < conf.Local.Count; i++ {
		period, _ := rl.Access()
		if period > 0 {
			t.Errorf("Period above zero: %v", period)
		}
	}

	if period, _ := rl.Access(); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}

	<-time.After(time.Millisecond * 15)

	for i := 0; i < conf.Local.Count; i++ {
		period, _ := rl.Access()
		if period != 0 {
			t.Errorf("Rate limited on get %v", i)
		}
	}

	if period, _ := rl.Access(); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

//------------------------------------------------------------------------------

func BenchmarkRateLimit(b *testing.B) {
	/* A rate limit is typically going to be protecting a networked resource
	 * where the request will likely be measured at least in hundreds of
	 * microseconds. It would be reasonable to assume the rate limit might be
	 * shared across tens of components.
	 *
	 * Therefore, we can probably sit comfortably with lock contention across
	 * one hundred or so parallel components adding an overhead of single digit
	 * microseconds. Since this benchmark doesn't take into account the actual
	 * request duration after receiving a rate limit I've set the number of
	 * components to ten in order to compensate.
	 */
	b.ReportAllocs()

	nParallel := 10
	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(nParallel)

	conf := NewConfig()
	conf.Local.Count = 1000
	conf.Local.Interval = "1ns"

	rl, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < nParallel; i++ {
		go func() {
			<-startChan
			for j := 0; j < b.N; j++ {
				period, _ := rl.Access()
				if period > 0 {
					time.Sleep(period)
				}
			}
			wg.Done()
		}()
	}

	b.ResetTimer()
	close(startChan)
	wg.Wait()
}

//------------------------------------------------------------------------------
