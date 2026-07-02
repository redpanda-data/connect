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

	"golang.org/x/time/rate"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	testLicenseBytesPerSec = 1 * 1024 * 1024 // 1 MB/s
	testLicenseWindowSec   = 30
	testLicenseBurstBytes  = testLicenseBytesPerSec * testLicenseWindowSec // 30 MB burst
)

// Throttler enforces the 1 MB/s compressed-bytes-out cap for enterprise
// features running under the embedded test license. All pipelines in the
// same process share one instance so load cannot be spread across pipelines
// to circumvent the cap.
type Throttler struct {
	limiter       *rate.Limiter
	throttleGauge *service.MetricGauge
	logger        *service.Logger
	direction     string // "input" or "output"
	throttling    atomic.Bool
}

func newThrottler(res *service.Resources, direction string) *Throttler {
	return &Throttler{
		limiter:       rate.NewLimiter(rate.Limit(testLicenseBytesPerSec), testLicenseBurstBytes),
		throttleGauge: res.Metrics().NewGauge("redpanda_connect_test_license_throttle_active"),
		logger:        res.Logger(),
		direction:     direction,
	}
}

// Wait blocks until the token bucket allows n bytes through. connector is the
// registered component name (e.g. "sap_hana") used in the throttle warning.
// If the cap is reached, a WARN is logged once and the metric is set to 1
// until throughput drops back below the limit.
//
// Oversized batches (n > burst) are billed in burst-sized chunks so that the
// full byte count is always charged — batching larger cannot bypass the cap.
func (t *Throttler) Wait(ctx context.Context, n int, connector string) error {
	if n <= 0 {
		return nil
	}

	anyDelay := false
	for n > 0 {
		charge := min(n, testLicenseBurstBytes)
		n -= charge

		r := t.limiter.ReserveN(time.Now(), charge)
		delay := r.Delay()
		if delay <= 0 {
			continue
		}

		anyDelay = true
		if t.throttling.CompareAndSwap(false, true) {
			t.logger.Warnf("Throughput cap reached under embedded test license (1 MB/s). Throttling enterprise %s %q. Apply a production license to remove the cap: https://docs.redpanda.com/redpanda-connect/get-started/licensing/", t.direction, connector)
			t.throttleGauge.Set(1)
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			r.Cancel()
			if t.throttling.CompareAndSwap(true, false) {
				t.throttleGauge.Set(0)
			}
			return ctx.Err()
		}
	}

	if !anyDelay {
		if t.throttling.CompareAndSwap(true, false) {
			t.throttleGauge.Set(0)
		}
	}
	return nil
}

// throttledBatchOutput wraps a service.BatchOutput and enforces the dev
// license egress cap before each WriteBatch call.
type throttledBatchOutput struct {
	wrapped   service.BatchOutput
	throttler *Throttler
	name      string
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
	if err := o.throttler.Wait(ctx, n, o.name); err != nil {
		return err
	}
	return o.wrapped.WriteBatch(ctx, batch)
}

func (o *throttledBatchOutput) Close(ctx context.Context) error {
	return o.wrapped.Close(ctx)
}

// throttledInput wraps a service.Input and enforces the dev
// license ingress cap after each Read call.
type throttledInput struct {
	wrapped   service.Input
	throttler *Throttler
	name      string
}

func (i *throttledInput) Connect(ctx context.Context) error { return i.wrapped.Connect(ctx) }

func (i *throttledInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	msg, ackFn, err := i.wrapped.Read(ctx)
	if err != nil || msg == nil {
		return msg, ackFn, err
	}
	if b, berr := msg.AsBytes(); berr == nil {
		if err := i.throttler.Wait(ctx, len(b), i.name); err != nil {
			return nil, nil, err
		}
	}
	return msg, ackFn, nil
}

func (i *throttledInput) Close(ctx context.Context) error { return i.wrapped.Close(ctx) }

// throttledBatchInput wraps a service.BatchInput and enforces the dev
// license ingress cap after each ReadBatch call.
type throttledBatchInput struct {
	wrapped   service.BatchInput
	throttler *Throttler
	name      string
}

func (i *throttledBatchInput) Connect(ctx context.Context) error {
	return i.wrapped.Connect(ctx)
}

func (i *throttledBatchInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	batch, ackFn, err := i.wrapped.ReadBatch(ctx)
	if err != nil || len(batch) == 0 {
		return batch, ackFn, err
	}
	var n int
	for _, msg := range batch {
		if b, berr := msg.AsBytes(); berr == nil {
			n += len(b)
		}
	}
	if err := i.throttler.Wait(ctx, n, i.name); err != nil {
		return nil, nil, err
	}
	return batch, ackFn, nil
}

func (i *throttledBatchInput) Close(ctx context.Context) error {
	return i.wrapped.Close(ctx)
}

// throttledOutput wraps a service.Output and enforces the dev
// license egress cap before each Write call.
type throttledOutput struct {
	wrapped   service.Output
	throttler *Throttler
	name      string
}

func (o *throttledOutput) Connect(ctx context.Context) error { return o.wrapped.Connect(ctx) }

func (o *throttledOutput) Write(ctx context.Context, msg *service.Message) error {
	if b, err := msg.AsBytes(); err == nil {
		if err := o.throttler.Wait(ctx, len(b), o.name); err != nil {
			return err
		}
	}
	return o.wrapped.Write(ctx, msg)
}

func (o *throttledOutput) Close(ctx context.Context) error { return o.wrapped.Close(ctx) }
