// Copyright 2026 Redpanda Data, Inc.
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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb"
)

// instrumentedInput is a minimal service.BatchInput we can interrogate
// after Ack to make sure Close was NOT called and that the actual ack
// callback fired with the right error.
type instrumentedInput struct {
	closeCalls atomic.Int32
}

func (*instrumentedInput) Connect(context.Context) error { return nil }
func (*instrumentedInput) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	return service.MessageBatch{service.NewMessage([]byte("x"))}, func(context.Context, error) error { return nil }, nil
}

func (i *instrumentedInput) Close(context.Context) error {
	i.closeCalls.Add(1)
	return nil
}

// TestInputAckInvokesStoredAckNotClose is the regression test for the
// bug where Ack() called Close() on the underlying component instead
// of invoking the per-batch ack callback the plugin handed back from
// ReadBatch. Verifies:
//   - the ack callback is invoked exactly once with the error
//     from the AckRequest
//   - Close is not called on the underlying component
//   - the (batch_id -> ackFn) mapping is removed from i.acks after Ack
func TestInputAckInvokesStoredAckNotClose(t *testing.T) {
	cmp := &instrumentedInput{}
	i := &input{component: cmp}

	var ackCalls atomic.Int32
	var ackErr error
	const batchID uint64 = 42
	i.acks.Store(batchID, service.AckFunc(func(_ context.Context, err error) error {
		ackCalls.Add(1)
		ackErr = err
		return nil
	}))

	// Ack with a non-nil error to also verify the error round-trips.
	nackedWith := errors.New("downstream failed")
	resp, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{
		BatchId: batchID,
		Error:   runtimepb.ErrorToProto(nackedWith),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Nil(t, resp.GetError(), "response should not carry an error when ack succeeded")

	assert.Equal(t, int32(1), ackCalls.Load(), "stored ack callback should fire exactly once")
	assert.Equal(t, int32(0), cmp.closeCalls.Load(), "Close must not be called on Ack")
	require.Error(t, ackErr)
	assert.Equal(t, nackedWith.Error(), ackErr.Error())

	_, stillThere := i.acks.Load(batchID)
	assert.False(t, stillThere, "ack callback should be removed after Ack")
}

// TestInputAckUnknownBatchID covers the "this batch isn't tracked"
// path — we want a clear error rather than a silent success.
func TestInputAckUnknownBatchID(t *testing.T) {
	i := &input{component: &instrumentedInput{}}

	resp, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: 999})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetError())
	assert.Contains(t, resp.GetError().GetMessage(), "unknown batch id 999")
}

// TestInputAckBeforeInit ensures we still surface ErrNotConnected when
// the plugin instance hasn't yet been initialised — that case path
// existed before the fix and should keep working.
func TestInputAckBeforeInit(t *testing.T) {
	i := &input{}

	resp, err := i.Ack(t.Context(), &runtimepb.BatchInputAckRequest{BatchId: 1})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.GetError())
	assert.NotNil(t, resp.GetError().GetNotConnected(), "expected ErrNotConnected payload before Init")
}
