// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforcegrpc

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
	"google.golang.org/grpc"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const testSchemaJSON = `{
	"type": "record",
	"name": "TestEvent",
	"fields": [{"name": "EventUuid", "type": "string"}]
}`

// fakeSubscribeStream serves a fixed sequence of FetchResponses, then blocks
// until its context is cancelled.
type fakeSubscribeStream struct {
	grpc.ClientStream // panics if any unimplemented method is called

	ctx   context.Context //nolint:containedctx // mirrors the grpc.ClientStream contract this fake stands in for
	mu    sync.Mutex
	queue []*FetchResponse
}

func (*fakeSubscribeStream) Send(*FetchRequest) error { return nil }

func (f *fakeSubscribeStream) Recv() (*FetchResponse, error) {
	f.mu.Lock()
	if len(f.queue) > 0 {
		resp := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()
		return resp, nil
	}
	f.mu.Unlock()
	<-f.ctx.Done()
	return nil, f.ctx.Err()
}

func (f *fakeSubscribeStream) Context() context.Context { return f.ctx }

func newBackpressureTestSubscription(t *testing.T, streamCtx context.Context, events int, bufferSize int) *Subscription {
	t.Helper()

	schema, err := avro.Parse(testSchemaJSON)
	require.NoError(t, err)

	payload, err := schema.Encode(map[string]any{"EventUuid": "u-1"})
	require.NoError(t, err)

	consumerEvents := make([]*ConsumerEvent, 0, events)
	for i := range events {
		consumerEvents = append(consumerEvents, &ConsumerEvent{
			Event:    &ProducerEvent{SchemaId: "s1", Payload: payload},
			ReplayId: []byte{byte(i + 1)},
		})
	}

	sc := NewSchemaCache(nil, "", "", "")
	sc.schemas["s1"] = schema

	s := &Subscription{
		client: &Client{
			log:         service.NewLoggerFromSlog(slog.Default()),
			schemaCache: sc,
		},
		config:      SubscriptionConfig{TopicName: "/event/Test__e", BatchSize: 10},
		eventBuffer: make(chan *PubSubEvent, bufferSize),
		done:        make(chan struct{}),
		state:       StreamStateConnected,
		stream: &fakeSubscribeStream{
			ctx:   streamCtx,
			queue: []*FetchResponse{{Events: consumerEvents, PendingNumRequested: 1}},
		},
	}
	return s
}

// TestReceiveLoopBackpressure verifies that a full event buffer blocks the
// receive loop instead of dropping events: with a buffer smaller than the
// batch, every event must still be delivered once the consumer drains, and
// none may be counted as dropped. (The pre-fix code dropped events on a full
// buffer while the batch's replay ID advanced past them - silent loss under
// downstream backpressure.)
func TestReceiveLoopBackpressure(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const events = 5
	s := newBackpressureTestSubscription(t, streamCtx, events, 1 /* buffer far smaller than the batch */)

	go s.receiveLoop(t.Context(), streamCtx)

	// The loop must not drop anything while the buffer is full: it blocks.
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, s.eventsDropped.Load(), "a full buffer must block, never drop")
	require.LessOrEqual(t, s.eventsReceived.Load(), int64(2), "only buffered events may be admitted while the consumer is stalled")

	// Draining the buffer releases the loop; every event arrives exactly once.
	var got []*PubSubEvent
	for range events {
		select {
		case ev := <-s.Events():
			got = append(got, ev)
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d was never delivered (dropped?)", len(got)+1)
		}
	}
	require.Len(t, got, events)
	require.Zero(t, s.eventsDropped.Load())
	// The received counter increments after each send completes, so it can
	// trail the drain momentarily.
	require.Eventually(t, func() bool {
		return s.eventsReceived.Load() == int64(events)
	}, 5*time.Second, 10*time.Millisecond)

	// Cancelling the stream context unblocks and terminates the loop. Mimic
	// Close(): the closing state must be set first so the loop shuts down
	// instead of reconnecting.
	s.mu.Lock()
	s.state = StreamStateClosing
	s.mu.Unlock()
	cancel()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit after stream context cancellation")
	}
}

// TestReceiveLoopBlockedSendEscapesOnClose verifies that a receive loop parked
// on a backpressured buffer send exits promptly when the stream context is
// cancelled, rather than leaking.
func TestReceiveLoopBlockedSendEscapesOnClose(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())

	s := newBackpressureTestSubscription(t, streamCtx, 3, 1)
	go s.receiveLoop(t.Context(), streamCtx)

	// Wait until the loop is parked on the full buffer.
	require.Eventually(t, func() bool {
		return s.eventsReceived.Load() >= 1
	}, 5*time.Second, 10*time.Millisecond)

	// Mimic Close(): set the closing state before cancelling.
	s.mu.Lock()
	s.state = StreamStateClosing
	s.mu.Unlock()
	cancel()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit after stream context cancellation")
	}
	require.Zero(t, s.eventsDropped.Load())
}

// TestRecordDecodeFailure verifies the consecutive-failure bookkeeping: the
// count trips only after maxConsecutiveDecodeFailures failures at the SAME
// replay position, resets when the failing position changes, and clears on a
// successful decode.
func TestRecordDecodeFailure(t *testing.T) {
	s := &Subscription{}

	for i := range maxConsecutiveDecodeFailures - 1 {
		require.False(t, s.recordDecodeFailure([]byte{0x01}), "failure %d must not trip the bound", i+1)
	}
	require.True(t, s.recordDecodeFailure([]byte{0x01}), "failure %d at one position must trip the bound", maxConsecutiveDecodeFailures)

	// A different position starts a fresh count.
	require.False(t, s.recordDecodeFailure([]byte{0x02}), "a new position must reset the count")

	// A successful decode clears everything.
	s.clearDecodeFailures()
	for i := range maxConsecutiveDecodeFailures - 1 {
		require.False(t, s.recordDecodeFailure([]byte{0x02}), "failure %d after a clear must not trip the bound", i+1)
	}
	require.True(t, s.recordDecodeFailure([]byte{0x02}))
}

// TestReceiveLoopTerminalDecodeFailure verifies that once an event at one
// replay position has exhausted the consecutive decode-failure bound, the
// receive loop fails the stream terminally - streamErr surfaces the failure
// and no reconnect is attempted - instead of redelivering forever.
func TestReceiveLoopTerminalDecodeFailure(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := newBackpressureTestSubscription(t, streamCtx, 1, 1)
	// Feed an undecodable payload for the known schema.
	s.stream = &fakeSubscribeStream{
		ctx: streamCtx,
		queue: []*FetchResponse{{
			Events: []*ConsumerEvent{{
				Event:    &ProducerEvent{SchemaId: "s1", Payload: []byte{0xff}},
				ReplayId: []byte{0x2a},
			}},
			PendingNumRequested: 1,
		}},
	}
	// This position has already failed on every prior redelivery; the next
	// failure exhausts the bound. (Earlier failures exercise the reconnect
	// path, which needs a real Pub/Sub connection - covered by the counting
	// test above.)
	s.decodeFailures = maxConsecutiveDecodeFailures - 1
	s.decodeFailureReplayID = []byte{0x2a}

	go s.receiveLoop(t.Context(), streamCtx)

	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit on a terminal decode failure")
	}
	require.ErrorContains(t, s.StreamErr(), "permanently undecodable",
		"the exhausted bound must surface a terminal stream error")
	s.mu.Lock()
	require.Equal(t, StreamStateDisconnected, s.state)
	s.mu.Unlock()
	require.Zero(t, s.reconnectCount.Load(), "a terminal decode failure must not reconnect")
}
