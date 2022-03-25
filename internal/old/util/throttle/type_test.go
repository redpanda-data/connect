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
	t.Skip("Tests are unpredictable on slow machines")
	t.Parallel()

	throt := New(
		OptMaxUnthrottledRetries(1),
		OptMaxExponentPeriod(time.Millisecond*500),
		OptThrottlePeriod(time.Millisecond*100),
	)

	errMargin := 0.05

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
	t.Skip("Tests are unpredictable on slow machines")
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
