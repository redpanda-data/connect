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

package throttle

import (
	"testing"
	"time"
)

func TestBasicThrottle(t *testing.T) {
	closeChan := make(chan struct{})
	close(closeChan)

	throt := New(
		OptMaxUnthrottledRetries(3),
		OptMaxExponentPeriod(time.Second*30),
		OptThrottlePeriod(time.Second),
		OptCloseChan(closeChan),
	)

	for i := 0; i < 3; i++ {
		if !throt.Retry() {
			t.Errorf("Throttle blocked early: %v", i)
		}
	}

	if throt.Retry() {
		t.Errorf("Throttle didn't throttle at end")
	}
}

func TestThrottleReset(t *testing.T) {
	closeChan := make(chan struct{})
	close(closeChan)

	throt := New(
		OptMaxUnthrottledRetries(3),
		OptMaxExponentPeriod(time.Second*30),
		OptThrottlePeriod(time.Second),
		OptCloseChan(closeChan),
	)

	for j := 0; j < 3; j++ {
		for i := 0; i < 3; i++ {
			if !throt.Retry() {
				t.Errorf("Throttle blocked early: %v", i)
			}
		}
		if throt.Retry() {
			t.Errorf("Throttle didn't throttle at end")
		}
		throt.Reset()
	}
}

func TestThrottleLinear(t *testing.T) {
	t.Parallel()

	throt := New(
		OptMaxUnthrottledRetries(1),
		OptMaxExponentPeriod(time.Millisecond*500),
		OptThrottlePeriod(time.Millisecond*100),
	)

	errMargin := 0.005

	tBefore := time.Now()
	throt.Retry()

	exp := time.Duration(0)
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	for i := 0; i < 5; i++ {
		tBefore = time.Now()
		throt.Retry()

		exp = time.Millisecond * 100
		if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
			t.Errorf("Unexpected retry period: %v != %v", act, exp)
		}
	}
}

func TestThrottleExponent(t *testing.T) {
	t.Parallel()

	base := time.Millisecond * 50

	throt := New(
		OptMaxUnthrottledRetries(1),
		OptMaxExponentPeriod(base*8),
		OptThrottlePeriod(base),
	)

	errMargin := 0.05

	tBefore := time.Now()
	throt.ExponentialRetry()

	exp := time.Duration(0)
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = base
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = base * 2
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = base * 4
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = base * 8
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = base * 8
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	throt.Reset()

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = time.Duration(0)
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = base
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}
}
