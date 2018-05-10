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
	throt := New(
		OptMaxUnthrottledRetries(1),
		OptMaxExponentPeriod(time.Millisecond*10),
		OptThrottlePeriod(time.Millisecond),
	)

	errMargin := 0.0005

	tBefore := time.Now()
	throt.Retry()

	exp := time.Duration(0)
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	for i := 0; i < 10; i++ {
		tBefore = time.Now()
		throt.Retry()

		exp = time.Millisecond
		if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
			t.Errorf("Unexpected retry period: %v != %v", act, exp)
		}
	}
}

func TestThrottleExponent(t *testing.T) {
	throt := New(
		OptMaxUnthrottledRetries(1),
		OptMaxExponentPeriod(time.Millisecond*10),
		OptThrottlePeriod(time.Millisecond),
	)

	errMargin := 0.0005

	tBefore := time.Now()
	throt.ExponentialRetry()

	exp := time.Duration(0)
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = time.Millisecond
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = time.Millisecond * 2
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = time.Millisecond * 4
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = time.Millisecond * 8
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = time.Millisecond * 10
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}

	tBefore = time.Now()
	throt.ExponentialRetry()

	exp = time.Millisecond * 10
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

	exp = time.Millisecond
	if act := time.Since(tBefore); (act - exp).Seconds() > errMargin {
		t.Errorf("Unexpected retry period: %v != %v", act, exp)
	}
}
