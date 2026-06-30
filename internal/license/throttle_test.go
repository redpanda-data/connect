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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// mockBatchOutput records WriteBatch calls for assertions.
type mockBatchOutput struct {
	batches []service.MessageBatch
}

func (*mockBatchOutput) Connect(_ context.Context) error { return nil }
func (*mockBatchOutput) Close(_ context.Context) error   { return nil }
func (m *mockBatchOutput) WriteBatch(_ context.Context, b service.MessageBatch) error {
	m.batches = append(m.batches, b)
	return nil
}

// mockBatchInput returns a single fixed batch per ReadBatch call.
type mockBatchInput struct {
	batch service.MessageBatch
}

func (*mockBatchInput) Connect(_ context.Context) error { return nil }
func (*mockBatchInput) Close(_ context.Context) error   { return nil }
func (m *mockBatchInput) ReadBatch(_ context.Context) (service.MessageBatch, service.AckFunc, error) {
	return m.batch, func(_ context.Context, _ error) error { return nil }, nil
}

func devLicenseResources(t *testing.T) *service.Resources {
	t.Helper()
	tmpDir := t.TempDir()
	res := service.MockResources()
	RegisterService(res, Config{
		customDefaultLicenseFilepath: filepath.Join(tmpDir, "missing.license"),
	})
	svc := getSharedService(res)
	require.True(t, svc.isTestLicense)
	return res
}

func TestWrapBatchOutput_NoopUnderProductionLicense(t *testing.T) {
	res := service.MockResources()
	InjectTestService(res) // enterprise, but NOT isTestLicense

	inner := &mockBatchOutput{}
	wrapped := WrapBatchOutput(res, inner)

	// Should be the same pointer — no wrapper applied.
	assert.Same(t, inner, wrapped.(*mockBatchOutput))
}

func TestWrapBatchOutput_ThrottlesUnderDevLicense(t *testing.T) {
	res := devLicenseResources(t)

	inner := &mockBatchOutput{}
	wrapped := WrapBatchOutput(res, inner)

	_, ok := wrapped.(*throttledBatchOutput)
	assert.True(t, ok, "expected throttledBatchOutput wrapper under dev license")
}

func TestWrapBatchInput_NoopUnderProductionLicense(t *testing.T) {
	res := service.MockResources()
	InjectTestService(res) // enterprise, but NOT isTestLicense

	msg := service.NewMessage([]byte("hello"))
	inner := &mockBatchInput{batch: service.MessageBatch{msg}}
	wrapped := WrapBatchInput(res, inner)

	assert.Same(t, inner, wrapped.(*mockBatchInput))
}

func TestWrapBatchInput_ThrottlesUnderDevLicense(t *testing.T) {
	res := devLicenseResources(t)

	msg := service.NewMessage([]byte("hello"))
	inner := &mockBatchInput{batch: service.MessageBatch{msg}}
	wrapped := WrapBatchInput(res, inner)

	_, ok := wrapped.(*throttledBatchInput)
	assert.True(t, ok, "expected throttledBatchInput wrapper under dev license")
}

func TestThrottler_PassthroughBelowCap(t *testing.T) {
	res := service.MockResources()
	throttler := newThrottler(res)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	// Small batch well under 1 MB/s cap — should pass with no delay.
	start := time.Now()
	err := throttler.Wait(ctx, 1024) // 1 KB
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestThrottler_ZeroBytesNoBlock(t *testing.T) {
	res := service.MockResources()
	throttler := newThrottler(res)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := throttler.Wait(ctx, 0)
	require.NoError(t, err)
}

func TestThrottler_CancelledContextReturnsError(t *testing.T) {
	res := service.MockResources()
	throttler := newThrottler(res)

	// Drain the entire burst bucket so the next Wait will block.
	ctx := t.Context()
	err := throttler.Wait(ctx, testLicenseBurstBytes)
	require.NoError(t, err)

	// Now the bucket is empty; any further bytes will block.
	cancelled, cancel := context.WithCancel(ctx)
	cancel() // cancel immediately

	err = throttler.Wait(cancelled, 1024)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestThrottler_EnforcesCap(t *testing.T) {
	res := service.MockResources()
	throttler := newThrottler(res)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Drain the entire burst bucket.
	err := throttler.Wait(ctx, testLicenseBurstBytes)
	require.NoError(t, err)

	// At 1 MB/s, sending 2 MB more must take at least ~2s.
	const extraBytes = 2 * testLicenseBytesPerSec
	start := time.Now()
	err = throttler.Wait(ctx, extraBytes)
	require.NoError(t, err)
	elapsed := time.Since(start)

	assert.Greater(t, elapsed, 1500*time.Millisecond, "throttler not enforcing 1 MB/s cap: sent 2 MB in %s", elapsed)
}
