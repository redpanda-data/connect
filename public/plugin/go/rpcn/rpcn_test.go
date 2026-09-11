// Copyright 2025 Redpanda Data, Inc.
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

package rpcn

import (
	"context"
	"errors"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBatchInput is a minimal service.BatchInput used only to give the
// input server a non-nil component, since Ack no longer calls any of its
// methods directly (that was the bug). closed tracks whether Close was
// called, so tests can assert Ack never triggers it; readBatch lets a test
// drive a real ReadBatch call when it needs a genuine BatchId rather than
// one poked directly into i.acks.
type fakeBatchInput struct {
	closed    *bool
	readBatch func(context.Context) (service.MessageBatch, service.AckFunc, error)
}

func (fakeBatchInput) Connect(context.Context) error { return nil }

func (f fakeBatchInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if f.readBatch != nil {
		return f.readBatch(ctx)
	}
	return nil, nil, nil
}

func (f fakeBatchInput) Close(context.Context) error {
	if f.closed != nil {
		*f.closed = true
	}
	return nil
}

func TestInputAck_RoutesToTheAckedBatchByID(t *testing.T) {
	i := &input{component: fakeBatchInput{}}

	var (
		ackCalled bool
		gotErr    error
	)
	myID := i.batchIDGenerator.Add(1)
	i.acks.Store(myID, service.AckFunc(func(_ context.Context, err error) error {
		ackCalled = true
		gotErr = err
		return nil
	}))

	resp, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: myID})
	require.NoError(t, err)
	assert.Nil(t, resp.Error)
	assert.True(t, ackCalled, "the AckFunc stored for this batch ID should have been called")
	assert.NoError(t, gotErr)
}

func TestInputAck_PropagatesTheNackErrorToTheCorrectBatch(t *testing.T) {
	i := &input{component: fakeBatchInput{}}

	nackErr := errors.New("downstream write failed")
	var gotErr error
	myID := i.batchIDGenerator.Add(1)
	i.acks.Store(myID, service.AckFunc(func(_ context.Context, err error) error {
		gotErr = err
		return nil
	}))

	_, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{
		BatchId: myID,
		Error:   runtimepb.ErrorToProto(nackErr),
	})
	require.NoError(t, err)
	require.Error(t, gotErr)
	assert.Equal(t, nackErr.Error(), gotErr.Error())
}

func TestInputAck_DoesNotAffectOtherOutstandingBatches(t *testing.T) {
	i := &input{component: fakeBatchInput{}}

	var otherAcked bool
	otherID := i.batchIDGenerator.Add(1)
	i.acks.Store(otherID, service.AckFunc(func(context.Context, error) error {
		otherAcked = true
		return nil
	}))

	targetID := i.batchIDGenerator.Add(1)
	i.acks.Store(targetID, service.AckFunc(func(context.Context, error) error { return nil }))

	_, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: targetID})
	require.NoError(t, err)
	assert.False(t, otherAcked, "acking one batch must not ack a different outstanding batch")

	// The other batch's ack should still be routable afterwards.
	_, err = i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: otherID})
	require.NoError(t, err)
	assert.True(t, otherAcked)
}

func TestInputAck_UnknownBatchIDReturnsAnError(t *testing.T) {
	i := &input{component: fakeBatchInput{}}

	resp, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: 12345})
	require.NoError(t, err)
	require.NotNil(t, resp.Error)
}

func TestInputAck_NeverClosesTheComponent(t *testing.T) {
	// Regression test for the original bug: Ack used to call Close on the
	// whole component instead of routing to the acked batch.
	closed := false
	i := &input{component: fakeBatchInput{closed: &closed}}

	myID := i.batchIDGenerator.Add(1)
	i.acks.Store(myID, service.AckFunc(func(context.Context, error) error { return nil }))

	_, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: myID})
	require.NoError(t, err)
	assert.False(t, closed, "Ack must not close the component")
}

// TestInputAck_RoutesViaTheRealBatchIDFromReadBatch drives a real ReadBatch
// call rather than poking a batch ID directly into i.acks: it's the
// BatchId ReadBatch actually hands back that Ack must key off. It also
// covers LoadAndDelete's deletion semantics (untested otherwise): acking
// the same batch ID twice must fail the second time, not silently ack (or
// re-ack) whatever AckFunc used to live there.
func TestInputAck_RoutesViaTheRealBatchIDFromReadBatch(t *testing.T) {
	var acked bool
	i := &input{component: fakeBatchInput{
		readBatch: func(context.Context) (service.MessageBatch, service.AckFunc, error) {
			return service.MessageBatch{service.NewMessage([]byte("hello"))}, func(context.Context, error) error {
				acked = true
				return nil
			}, nil
		},
	}}

	readResp, err := i.ReadBatch(t.Context(), &runtimepb.BatchInputReadRequest{})
	require.NoError(t, err)
	require.Nil(t, readResp.Error)

	ackResp, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: readResp.BatchId})
	require.NoError(t, err)
	assert.Nil(t, ackResp.Error)
	assert.True(t, acked, "acking the BatchId ReadBatch actually returned should route to that batch's AckFunc")

	ackResp, err = i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: readResp.BatchId})
	require.NoError(t, err)
	assert.NotNil(t, ackResp.Error, "acking an already-acked batch ID again must fail, not silently succeed")
}

// TestInputAck_NilAckFuncReturnsAnErrorInsteadOfPanicking guards against a
// plugin author's ReadBatch returning a nil AckFunc alongside a nil error
// (valid per the service.BatchInput contract, if unusual) — Ack must
// report that cleanly rather than panicking the whole plugin subprocess
// when it eventually tries to invoke a nil func value.
func TestInputAck_NilAckFuncReturnsAnErrorInsteadOfPanicking(t *testing.T) {
	i := &input{component: fakeBatchInput{
		readBatch: func(context.Context) (service.MessageBatch, service.AckFunc, error) {
			return service.MessageBatch{service.NewMessage([]byte("hello"))}, nil, nil
		},
	}}

	readResp, err := i.ReadBatch(t.Context(), &runtimepb.BatchInputReadRequest{})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		ackResp, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: readResp.BatchId})
		require.NoError(t, err)
		assert.NotNil(t, ackResp.Error, "a nil AckFunc should produce an error response, not a panic")
	})
}
