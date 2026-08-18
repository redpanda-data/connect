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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	// A successful decode at the FAILING position clears everything.
	s.clearDecodeFailures([]byte{0x02})
	for i := range maxConsecutiveDecodeFailures - 1 {
		require.False(t, s.recordDecodeFailure([]byte{0x02}), "failure %d after a clear must not trip the bound", i+1)
	}
	require.True(t, s.recordDecodeFailure([]byte{0x02}))

	// A success at an UNRELATED position must NOT clear the tracked failure:
	// reconnects redeliver the whole batch, so decodable events before an
	// undecodable one succeed on every cycle - clearing on them would keep the
	// count below the bound forever.
	s.clearDecodeFailures([]byte{0x02})
	for i := range maxConsecutiveDecodeFailures - 1 {
		require.False(t, s.recordDecodeFailure([]byte{0x03}), "failure %d must not trip the bound", i+1)
		s.clearDecodeFailures([]byte{0x01}) // prefix event succeeding again
	}
	require.True(t, s.recordDecodeFailure([]byte{0x03}),
		"an unrelated success must not reset the failing position's count")
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
	// An anchor exists (a previous event was delivered), so the failure takes
	// the bounded path. This position has already failed on every prior
	// redelivery; the next failure exhausts the bound. (Earlier failures
	// exercise the reconnect path, which needs a real Pub/Sub connection -
	// covered by the counting test above.)
	s.lastReplayID = []byte{0x29}
	s.decodeFailures = positionFailures{count: maxConsecutiveDecodeFailures - 1, replayID: []byte{0x2a}}

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

// TestReceiveLoopUnanchoredDecodeFailureIsTerminal verifies the fresh-stream
// case: with no replay anchor a reconnect would fall back to the configured
// preset (LATEST drops the batch silently), so an undecodable first event
// must fail the stream terminally and loudly instead of reconnecting.
func TestReceiveLoopUnanchoredDecodeFailureIsTerminal(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := newBackpressureTestSubscription(t, streamCtx, 1, 1)
	s.stream = &fakeSubscribeStream{
		ctx: streamCtx,
		queue: []*FetchResponse{{
			Events: []*ConsumerEvent{{
				Event:    &ProducerEvent{SchemaId: "s1", Payload: []byte{0xff}},
				ReplayId: []byte{0x01},
			}},
			PendingNumRequested: 1,
		}},
	}
	// No lastReplayID: nothing has ever been delivered on this stream.

	go s.receiveLoop(t.Context(), streamCtx)

	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit on an unanchored decode failure")
	}
	require.ErrorContains(t, s.StreamErr(), "no replay anchor",
		"an unanchored decode failure must fail terminally, never reconnect via the preset")
	require.Zero(t, s.reconnectCount.Load())
}

// TestReceiveLoopAdvancesAnchorPerEvent verifies that the replay anchor moves
// past each event as it is buffered, so a mid-batch failure redelivers from
// exactly the failing event rather than re-emitting the batch prefix (or, on
// a fresh stream, losing the batch entirely to a preset reconnect).
func TestReceiveLoopAdvancesAnchorPerEvent(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const events = 3
	s := newBackpressureTestSubscription(t, streamCtx, events, events)
	go s.receiveLoop(t.Context(), streamCtx)

	for i := range events {
		select {
		case <-s.Events():
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d was never delivered", i+1)
		}
	}
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.lastReplayID) > 0 && s.lastReplayID[0] == byte(events)
	}, 5*time.Second, 10*time.Millisecond, "the anchor must advance to the last buffered event's replay ID")

	s.mu.Lock()
	s.state = StreamStateClosing
	s.mu.Unlock()
	cancel()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit")
	}
}

// TestReceiveLoopTerminalDecodeFailureAfterDecodablePrefix verifies the bound
// still trips when the undecodable event is preceded by a decodable one in the
// same batch - the realistic BatchSize>1 shape. Each redelivery decodes the
// prefix successfully; that success must not reset the failing event's count.
func TestReceiveLoopTerminalDecodeFailureAfterDecodablePrefix(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := newBackpressureTestSubscription(t, streamCtx, 1, 2)

	schema, err := avro.Parse(testSchemaJSON)
	require.NoError(t, err)
	goodPayload, err := schema.Encode(map[string]any{"EventUuid": "u-good"})
	require.NoError(t, err)

	s.stream = &fakeSubscribeStream{
		ctx: streamCtx,
		queue: []*FetchResponse{{
			Events: []*ConsumerEvent{
				{Event: &ProducerEvent{SchemaId: "s1", Payload: goodPayload}, ReplayId: []byte{0x01}},
				{Event: &ProducerEvent{SchemaId: "s1", Payload: []byte{0xff}}, ReplayId: []byte{0x2a}},
			},
			PendingNumRequested: 1,
		}},
	}
	// The undecodable event has already failed on every prior redelivery; this
	// delivery decodes the prefix again and then exhausts the bound.
	s.decodeFailures = positionFailures{count: maxConsecutiveDecodeFailures - 1, replayID: []byte{0x2a}}

	go s.receiveLoop(t.Context(), streamCtx)

	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit on a terminal decode failure")
	}
	require.ErrorContains(t, s.StreamErr(), "permanently undecodable",
		"the decodable prefix must not reset the failing event's count")
	require.Zero(t, s.reconnectCount.Load(), "a terminal decode failure must not reconnect")
}

// failingSchemaPubSub satisfies PubSubClient via embedding; only GetSchema is
// ever called by SchemaCache on the paths under test.
type failingSchemaPubSub struct{ PubSubClient }

func (failingSchemaPubSub) GetSchema(context.Context, *SchemaRequest, ...grpc.CallOption) (*SchemaInfo, error) {
	return nil, errors.New("schema endpoint down")
}

// TestReceiveLoopUnanchoredSchemaRetryHonorsReconnectPolicy verifies that a
// schema-fetch failure on a fresh stream (no replay anchor, so reconnecting
// via the preset could drop the batch) retries inline under the configured
// reconnect policy and, once reconnect_max_attempts is exhausted, fails the
// stream terminally with an error naming the schema stage - never silently.
func TestReceiveLoopUnanchoredSchemaRetryHonorsReconnectPolicy(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := newBackpressureTestSubscription(t, streamCtx, 1, 1)
	s.client.schemaCache = NewSchemaCache(failingSchemaPubSub{}, "", "", "")
	s.client.baseBackoff = time.Millisecond
	s.client.maxBackoff = 2 * time.Millisecond
	s.client.maxReconnect = 3

	s.stream = &fakeSubscribeStream{
		ctx: streamCtx,
		queue: []*FetchResponse{{
			Events: []*ConsumerEvent{{
				Event:    &ProducerEvent{SchemaId: "s-unknown", Payload: []byte{0x01}},
				ReplayId: []byte{0x01},
			}},
			PendingNumRequested: 1,
		}},
	}

	go s.receiveLoop(t.Context(), streamCtx)

	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit after the schema retry budget was exhausted")
	}
	require.ErrorContains(t, s.StreamErr(), "retry budget is exhausted",
		"the terminal error must name the exhausted retry budget")
	require.ErrorContains(t, s.StreamErr(), "fetching schema",
		"the terminal error must name the schema stage, not the payload")
	require.Zero(t, s.reconnectCount.Load(), "an unanchored schema failure must never reconnect via the preset")
}

// TestRecordSchemaFailure verifies the schema-fetch bound: transient
// failures are governed by the reconnect policy (0 = indefinite retries),
// deterministic failures trip at maxConsecutiveDecodeFailures regardless of
// policy - including under the shipped default of unlimited reconnects - and
// counts are per replay position with position-scoped clearing.
func TestRecordSchemaFailure(t *testing.T) {
	s := &Subscription{client: &Client{maxReconnect: 3}}

	require.False(t, s.recordSchemaFailure([]byte{0x01}, false))
	require.False(t, s.recordSchemaFailure([]byte{0x01}, false))
	require.True(t, s.recordSchemaFailure([]byte{0x01}, false), "the third transient failure at one position must exhaust reconnect_max_attempts=3")

	// A new position starts fresh.
	require.False(t, s.recordSchemaFailure([]byte{0x02}, false))
	// A success at an unrelated position must not reset the tracked count.
	s.clearSchemaFailures([]byte{0x09})
	require.False(t, s.recordSchemaFailure([]byte{0x02}, false))
	// A success at the tracked position clears it.
	s.clearSchemaFailures([]byte{0x02})
	require.False(t, s.recordSchemaFailure([]byte{0x02}, false))

	// Unlimited policy: transient failures never trip...
	unlimited := &Subscription{client: &Client{maxReconnect: 0}}
	for range 100 {
		require.False(t, unlimited.recordSchemaFailure([]byte{0x01}, false),
			"reconnect_max_attempts=0 retries transient failures indefinitely")
	}
	// ...but deterministic failures still trip at the decode bound: the
	// shipped default must not leave an unfetchable schema livelocking.
	fresh := &Subscription{client: &Client{maxReconnect: 0}}
	for i := range maxConsecutiveDecodeFailures - 1 {
		require.False(t, fresh.recordSchemaFailure([]byte{0x03}, true), "failure %d must not trip yet", i+1)
	}
	require.True(t, fresh.recordSchemaFailure([]byte{0x03}, true),
		"a deterministic schema failure must terminate even under unlimited reconnects")
}

// TestDeterministicSchemaFailure locks in the failure classification.
func TestDeterministicSchemaFailure(t *testing.T) {
	require.True(t, deterministicSchemaFailure(status.Error(codes.NotFound, "no such schema")))
	require.True(t, deterministicSchemaFailure(status.Error(codes.InvalidArgument, "bad schema id")))
	require.True(t, deterministicSchemaFailure(status.Error(codes.PermissionDenied, "nope")))
	require.True(t, deterministicSchemaFailure(errors.New("parse schema x: invalid avro")), "a compile failure has no gRPC status and cannot heal")
	require.False(t, deterministicSchemaFailure(status.Error(codes.Unavailable, "endpoint down")))
	require.False(t, deterministicSchemaFailure(status.Error(codes.Unauthenticated, "token expired")))
	require.False(t, deterministicSchemaFailure(context.Canceled))
	require.False(t, deterministicSchemaFailure(fmt.Errorf("fetch schema: %w", context.DeadlineExceeded)))
}

// TestReceiveLoopAnchoredSchemaFailureExhaustsReconnectPolicy verifies that a
// deterministically unfetchable schema on an ANCHORED stream fails terminally
// once the per-position count exhausts reconnect_max_attempts, instead of
// reconnect-redelivering forever (each reconnect succeeds, so the per-call
// reconnect budget alone can never trip).
func TestReceiveLoopAnchoredSchemaFailureExhaustsReconnectPolicy(t *testing.T) {
	streamCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s := newBackpressureTestSubscription(t, streamCtx, 1, 1)
	s.client.schemaCache = NewSchemaCache(failingSchemaPubSub{}, "", "", "")
	s.client.maxReconnect = 3

	s.stream = &fakeSubscribeStream{
		ctx: streamCtx,
		queue: []*FetchResponse{{
			Events: []*ConsumerEvent{{
				Event:    &ProducerEvent{SchemaId: "s-broken", Payload: []byte{0x01}},
				ReplayId: []byte{0x2a},
			}},
			PendingNumRequested: 1,
		}},
	}
	// Anchored (an earlier event was delivered), and this position has already
	// failed on every prior redelivery cycle; the next failure exhausts the
	// budget. The stub's error carries no gRPC status, so it classifies as
	// deterministic and is bounded by maxConsecutiveDecodeFailures. (The
	// intermediate reconnect hops need a real Pub/Sub connection, covered by
	// the counting test above.)
	s.lastReplayID = []byte{0x29}
	s.schemaFailures = positionFailures{count: maxConsecutiveDecodeFailures - 1, replayID: []byte{0x2a}}

	go s.receiveLoop(t.Context(), streamCtx)

	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive loop did not exit after the schema budget was exhausted")
	}
	require.ErrorContains(t, s.StreamErr(), "failed repeatedly",
		"a deterministically unfetchable schema must eventually surface a terminal error")
	require.ErrorContains(t, s.StreamErr(), "fetching schema")
	require.Zero(t, s.reconnectCount.Load(), "the exhausted budget must fail terminally, not reconnect again")
}
