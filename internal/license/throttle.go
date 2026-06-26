// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/time/rate"
)

const (
	testLicenseBytesPerSec  = 1 * 1024 * 1024 // 1 MB/s
	testLicenseWindowSec    = 30
	testLicenseBurstBytes   = testLicenseBytesPerSec * testLicenseWindowSec // 30 MB burst
)

// Throttler enforces the 1 MB/s compressed-bytes-out cap for enterprise
// features running under the embedded test license. All pipelines in the
// same process share one instance so load cannot be spread across pipelines
// to circumvent the cap.
type Throttler struct {
	limiter       *rate.Limiter
	throttleGauge *service.MetricGauge
	logger        *service.Logger
	throttling    atomic.Bool
}

func newThrottler(res *service.Resources) *Throttler {
	return &Throttler{
		limiter:       rate.NewLimiter(rate.Limit(testLicenseBytesPerSec), testLicenseBurstBytes),
		throttleGauge: res.Metrics().NewGauge("redpanda_connect_test_license_throttle_active"),
		logger:        res.Logger(),
	}
}

// Wait blocks until the token bucket allows n bytes through. If the cap is
// reached, a WARN is logged once and the metric is set to 1 until throughput
// drops back below the limit.
func (t *Throttler) Wait(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	// Cap n to burst to avoid a panic in ReserveN when a single batch exceeds
	// the 30 MB window. Very large batches are undercharged slightly; the cap
	// remains effective at sustained rates.
	if n > testLicenseBurstBytes {
		n = testLicenseBurstBytes
	}

	r := t.limiter.ReserveN(time.Now(), n)
	delay := r.Delay()
	if delay <= 0 {
		if t.throttling.CompareAndSwap(true, false) {
			t.throttleGauge.Set(0)
		}
		return nil
	}

	if t.throttling.CompareAndSwap(false, true) {
		t.logger.Warn("Throughput cap reached under embedded test license (1 MB/s). Throttling enterprise output. Apply a production license to remove the cap: https://docs.redpanda.com/redpanda-connect/get-started/licensing/")
		t.throttleGauge.Set(1)
	}

	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		r.Cancel()
		if t.throttling.CompareAndSwap(true, false) {
			t.throttleGauge.Set(0)
		}
		return ctx.Err()
	}
}

// throttledBatchOutput wraps a service.BatchOutput and enforces the test
// license throughput cap before each WriteBatch call.
type throttledBatchOutput struct {
	wrapped   service.BatchOutput
	throttler *Throttler
}

func (o *throttledBatchOutput) Connect(ctx context.Context) error {
	return o.wrapped.Connect(ctx)
}

func (o *throttledBatchOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	var n int
	for _, msg := range batch {
		b, err := msg.AsBytes()
		if err == nil {
			n += len(b)
		}
	}
	if err := o.throttler.Wait(ctx, n); err != nil {
		return err
	}
	return o.wrapped.WriteBatch(ctx, batch)
}

func (o *throttledBatchOutput) Close(ctx context.Context) error {
	return o.wrapped.Close(ctx)
}
