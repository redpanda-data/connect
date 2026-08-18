// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforcegrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// subscribeSettleDelay is how long after a successful subscribe Send we wait
// before signalling readiness. Salesforce acknowledges the stream immediately
// at the gRPC layer, but routing the subscription internally takes additional
// time; events published in that window are dropped when using
// ReplayPreset_LATEST. Empirically 3s is sufficient on the Pub/Sub API; 5s
// provides safety margin without meaningfully slowing startup.
const subscribeSettleDelay = 5 * time.Second

// maxConsecutiveDecodeFailures bounds redelivery attempts for an event whose
// Avro decode keeps failing at the same replay position. A decode failure
// against a successfully fetched schema is deterministic, and the replay
// anchor advances per delivered event, so each reconnect redelivers from
// exactly the failing event; once the same position has failed this many
// times in a row the payload is treated as permanently undecodable and the
// stream fails loudly. Schema-fetch failures never count toward this bound -
// they are transport-class errors governed by the reconnect policy.
const maxConsecutiveDecodeFailures = 5

// Subscription owns one subscribe stream for a single Pub/Sub topic. It reuses
// the parent Client's connection, auth, and schema cache.
type Subscription struct {
	client *Client
	isCDC  bool // computed once at construction from config.TopicName

	mu           sync.Mutex
	config       SubscriptionConfig
	stream       PubSub_SubscribeClient
	lastReplayID []byte
	eventBuffer  chan *PubSubEvent
	cancel       context.CancelFunc
	done         chan struct{}
	ready        chan struct{}
	streamErr    error
	state        StreamState
	// decodeFailures and schemaFailures count consecutive failures pinned to
	// one replay position each (guarded by mu): deterministic Avro-decode
	// failures bounded by maxConsecutiveDecodeFailures, and schema-fetch
	// failures bounded by the reconnect policy (reconnect_max_attempts).
	// Position-scoped so an unrelated event's success never resets them.
	decodeFailures positionFailures
	schemaFailures positionFailures

	// Atomic counters for health reporting.
	eventsReceived     atomic.Int64
	eventsDropped      atomic.Int64
	eventsDecodeErrors atomic.Int64
	reconnectCount     atomic.Int64
	lastEventTime      atomic.Int64
	lastError          atomic.Value
	lastErrorTime      atomic.Int64
}

// connect opens a new subscribe stream. Caller must not hold s.mu.
func (s *Subscription) connect(ctx context.Context, replayPreset ReplayPreset, replayID []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.config.ReplayPreset = replayPreset
	if len(replayID) > 0 {
		s.lastReplayID = replayID
	}
	return s.connectLocked(ctx)
}

// connectLocked opens a new stream and starts the receive loop. Caller must
// hold s.mu.
func (s *Subscription) connectLocked(ctx context.Context) error {
	s.state = StreamStateConnecting
	s.streamErr = nil

	streamCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	token, instanceURL, tenantID := s.client.auth()
	md := metadata.Pairs(
		"accesstoken", token,
		"instanceurl", instanceURL,
		"tenantid", tenantID,
	)
	streamCtx = metadata.NewOutgoingContext(streamCtx, md)

	stream, err := s.client.pubsub.Subscribe(streamCtx)
	if err != nil {
		cancel()
		s.state = StreamStateDisconnected
		return fmt.Errorf("open subscribe stream: %w", err)
	}
	s.stream = stream

	fetchReq := &FetchRequest{
		TopicName:    s.config.TopicName,
		NumRequested: s.config.BatchSize,
	}
	if len(s.lastReplayID) > 0 {
		fetchReq.ReplayPreset = ReplayPreset_CUSTOM
		fetchReq.ReplayId = s.lastReplayID
	} else {
		fetchReq.ReplayPreset = s.config.ReplayPreset
	}

	if err := stream.Send(fetchReq); err != nil {
		cancel()
		s.state = StreamStateDisconnected
		return fmt.Errorf("send initial FetchRequest: %w", err)
	}

	s.client.log.Infof("Pub/Sub subscription started on topic %s (preset=%v)", s.config.TopicName, fetchReq.ReplayPreset)
	s.state = StreamStateConnected

	s.done = make(chan struct{})

	// Subscription is established once the server accepts the initial
	// FetchRequest. Salesforce does not send a response until an event arrives,
	// so waiting for the first Recv would block indefinitely on idle topics.
	s.markReadyLocked()

	go s.receiveLoop(ctx, streamCtx)

	return nil
}

// anchored reports whether a replay resume position exists: only then can a
// reconnect redeliver the current batch (CUSTOM replay is exclusive-after).
// On a fresh stream with no anchor, a reconnect falls back to the configured
// preset - with LATEST that silently drops the batch, so unanchored failures
// must never take the reconnect path.
func (s *Subscription) anchored() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lastReplayID) > 0
}

// failTerminal fails the stream permanently: streamErr surfaces through the
// health tick and no reconnect is attempted.
func (s *Subscription) failTerminal(err error) {
	s.lastError.Store(err)
	s.lastErrorTime.Store(time.Now().UnixNano())
	s.mu.Lock()
	s.streamErr = err
	s.state = StreamStateDisconnected
	s.mu.Unlock()
	s.client.log.Errorf("Pub/Sub stream failed permanently (topic=%s): %v", s.config.TopicName, err)
}

// failDecode routes an Avro decode failure: reconnect-and-redeliver while the
// position is under the consecutive-failure bound, terminal stream failure
// once it is exceeded. Schema-fetch failures do NOT come through here - they
// are transport-class errors governed by the reconnect policy, not evidence
// of an undecodable payload.
func (s *Subscription) failDecode(replayID []byte, failStream func(error), err error) {
	if !s.recordDecodeFailure(replayID) {
		failStream(err)
		return
	}
	s.failTerminal(fmt.Errorf("decoding event at replay position %x: %d consecutive failures, treating as permanently undecodable: %w", replayID, maxConsecutiveDecodeFailures, err))
}

// positionFailures counts consecutive failures pinned to one replay
// position. A failure at a different position resets the count (the stream
// has moved on); a success clears it only when it lands at the tracked
// position - successes elsewhere must not reset it, since a redelivery cycle
// decodes the events around a permanently failing one successfully every
// time, which would keep the count oscillating below any bound forever.
// Callers must hold Subscription.mu.
type positionFailures struct {
	count    int
	replayID []byte
}

func (p *positionFailures) record(replayID []byte) int {
	if !bytes.Equal(replayID, p.replayID) {
		p.replayID = append([]byte(nil), replayID...)
		p.count = 0
	}
	p.count++
	return p.count
}

func (p *positionFailures) clear(replayID []byte) {
	if p.replayID != nil && bytes.Equal(replayID, p.replayID) {
		p.count = 0
		p.replayID = nil
	}
}

// recordDecodeFailure counts an Avro-decode failure at the given replay
// position and reports whether the position has now failed
// maxConsecutiveDecodeFailures times in a row.
func (s *Subscription) recordDecodeFailure(replayID []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.decodeFailures.record(replayID) >= maxConsecutiveDecodeFailures
}

// clearDecodeFailures resets the decode-failure count when the event at the
// tracked failing position decodes successfully.
func (s *Subscription) clearDecodeFailures(replayID []byte) {
	s.mu.Lock()
	s.decodeFailures.clear(replayID)
	s.mu.Unlock()
}

// recordSchemaFailure counts a schema-fetch failure at the given replay
// position against the reconnect policy: with reconnect_max_attempts 0
// (unlimited) it never trips - the user asked for indefinite retries on
// transport-class failures - but a bounded policy must also bound this path.
// Each reconnect here succeeds (the transport is healthy, the schema is
// not), so reconnectWithBackoff's own per-call budget never accumulates: a
// deterministically unfetchable schema (uncompilable SchemaJson, permanent
// NotFound) would otherwise reconnect-redeliver forever without ever
// surfacing an error, the same livelock the decode bound exists for.
func (s *Subscription) recordSchemaFailure(replayID []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.client.maxReconnect
	return budget > 0 && s.schemaFailures.record(replayID) >= budget
}

// clearSchemaFailures resets the schema-failure count when a fetch succeeds
// at the tracked failing position.
func (s *Subscription) clearSchemaFailures(replayID []byte) {
	s.mu.Lock()
	s.schemaFailures.clear(replayID)
	s.mu.Unlock()
}

// receiveLoop reads from the gRPC stream and pushes decoded events into the
// buffer. On stream errors it attempts reconnection with backoff instead of
// exiting. streamCtx is this stream's cancellation context: it unblocks a
// backpressured buffer send when the subscription closes or reconnects.
func (s *Subscription) receiveLoop(ctx, streamCtx context.Context) {
	// Capture done at goroutine start. reconnectWithBackoff → connectLocked
	// replaces s.done with a fresh channel for the new goroutine; closing the
	// old reference here prevents a double-close panic when both goroutines
	// eventually return.
	done := s.done
	defer close(done)

	// failStream logs err and hands control to the reconnect path. Because
	// s.lastReplayID has not been advanced past the current batch, the
	// reconnected stream redelivers it: duplicates, never loss.
	failStream := func(err error) {
		s.client.log.Errorf("Pub/Sub stream error (topic=%s), reconnecting: %v", s.config.TopicName, err)
		s.lastError.Store(err)
		s.lastErrorTime.Store(time.Now().UnixNano())

		if reconnErr := s.reconnectWithBackoff(ctx); reconnErr != nil {
			s.mu.Lock()
			s.streamErr = reconnErr
			s.state = StreamStateDisconnected
			s.mu.Unlock()
			s.client.log.Errorf("Reconnection failed permanently (topic=%s): %v", s.config.TopicName, reconnErr)
		}
	}

	for {
		resp, err := s.stream.Recv()
		if err != nil {
			s.mu.Lock()
			if s.state == StreamStateClosing {
				s.streamErr = err
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()

			failStream(err)
			return
		}

		if len(resp.Events) == 0 {
			if len(resp.LatestReplayId) > 0 {
				s.mu.Lock()
				s.lastReplayID = resp.LatestReplayId
				s.mu.Unlock()
			}
			continue
		}

		for _, consumerEvent := range resp.Events {
			event := consumerEvent.Event
			if event == nil {
				continue
			}

			// A schema fetch or decode failure must not skip the event: the
			// batch's replay ID would advance past it and the event would be
			// silently lost. When a replay anchor exists, reconnect instead —
			// lastReplayID advances per delivered event, so redelivery resumes
			// exactly at the failing event. Both failure classes are bounded
			// per replay position: Avro decode failures against a fetched
			// schema (deterministic) by maxConsecutiveDecodeFailures, and
			// schema-fetch failures by the reconnect policy itself
			// (reconnect_max_attempts; 0 = retry forever) — counted here
			// because each individual reconnect succeeds, so the per-call
			// reconnect budget alone can never trip. On a fresh stream with
			// NO anchor a reconnect would fall back to the configured preset
			// and could silently drop the batch (LATEST), so schema fetches
			// retry inline under the same policy and decode failures fail the
			// stream terminally instead.
			schema, err := s.client.schemaCache.GetSchema(ctx, event.SchemaId)
			if err != nil && !s.anchored() {
				// A reconnect here would resume via the configured preset and
				// could silently drop this batch (LATEST), so the fetch retries
				// inline instead - governed by the same reconnect policy a
				// reconnect would use (reconnect_min_delay/_max_delay and
				// reconnect_max_attempts; 0 = retry indefinitely).
				for attempt := 0; err != nil; attempt++ {
					if s.client.maxReconnect > 0 && attempt >= s.client.maxReconnect {
						break
					}
					delay := grpcBackoffWithJitter(s.client.baseBackoff, s.client.maxBackoff, attempt)
					s.client.log.Warnf("Schema fetch failed on a fresh stream with no replay anchor to redeliver from (topic=%s, schemaID=%s), retrying inline in %v (attempt %d): %v", s.config.TopicName, event.SchemaId, delay, attempt+1, err)
					t := time.NewTimer(delay)
					select {
					case <-t.C:
					case <-streamCtx.Done():
						t.Stop()
						return
					case <-ctx.Done():
						t.Stop()
						return
					}
					schema, err = s.client.schemaCache.GetSchema(ctx, event.SchemaId)
				}
				if err != nil {
					s.eventsDecodeErrors.Add(1)
					s.failTerminal(fmt.Errorf("fetching schema for the first event of a fresh stream (schemaID=%s): no replay anchor to redeliver from and reconnect_max_attempts (%d) exhausted: %w", event.SchemaId, s.client.maxReconnect, err))
					return
				}
			}
			if err != nil {
				s.eventsDecodeErrors.Add(1)
				// Bounded by the reconnect policy: each reconnect below
				// succeeds (the transport is healthy), so the per-call
				// reconnect budget never accumulates - the per-position count
				// is what makes reconnect_max_attempts meaningful for a
				// deterministically unfetchable schema. 0 = retry forever.
				if s.recordSchemaFailure(consumerEvent.ReplayId) {
					s.failTerminal(fmt.Errorf("fetching schema (schemaID=%s) failed %d consecutive times at replay position %x (reconnect_max_attempts exhausted): %w", event.SchemaId, s.client.maxReconnect, consumerEvent.ReplayId, err))
					return
				}
				failStream(fmt.Errorf("get schema for event (schemaID=%s): %w", event.SchemaId, err))
				return
			}
			s.clearSchemaFailures(consumerEvent.ReplayId)

			decoded, err := DecodeAvroPayload(schema, event.Payload)
			if err != nil {
				s.eventsDecodeErrors.Add(1)
				if !s.anchored() {
					s.failTerminal(fmt.Errorf("decoding the first event of a fresh stream (schemaID=%s, replay position %x): payload is undecodable and no replay anchor exists to redeliver from: %w", event.SchemaId, consumerEvent.ReplayId, err))
					return
				}
				s.failDecode(consumerEvent.ReplayId, failStream, fmt.Errorf("decode Avro payload (schemaID=%s): %w", event.SchemaId, err))
				return
			}
			s.clearDecodeFailures(consumerEvent.ReplayId)

			pubsubEvent := &PubSubEvent{
				ReplayID:   consumerEvent.ReplayId,
				SchemaID:   event.SchemaId,
				TopicName:  s.config.TopicName,
				RawPayload: decoded,
			}

			if s.isCDC {
				pubsubEvent.Type = EventTypeCDC
				extractCDCFields(pubsubEvent, decoded)
			} else {
				pubsubEvent.Type = EventTypePlatform
				pubsubEvent.Fields = decoded
				if uuid, ok := UnwrapAvroString(decoded["EventUuid"]); ok && uuid != "" {
					pubsubEvent.EventUUID = uuid
				}
			}

			// A full buffer applies backpressure instead of dropping: while
			// this send blocks, no flow-control FetchRequest is issued, so
			// Salesforce stops sending and the replay cursor cannot advance
			// past an undelivered event. The stream context unblocks the send
			// on close or reconnect.
			select {
			case s.eventBuffer <- pubsubEvent:
				s.eventsReceived.Add(1)
				s.lastEventTime.Store(time.Now().UnixNano())
				// Advance the replay anchor past this event: CUSTOM replay is
				// exclusive-after, so a reconnect now redelivers from exactly
				// the next (possibly failing) event - no lost first batch, no
				// re-emitted prefix.
				s.mu.Lock()
				s.lastReplayID = pubsubEvent.ReplayID
				s.mu.Unlock()
				s.client.log.Debugf("Pub/Sub event received (topic=%s, schemaID=%s, replayID=%x)", pubsubEvent.TopicName, pubsubEvent.SchemaID, pubsubEvent.ReplayID)
			case <-streamCtx.Done():
				return
			case <-ctx.Done():
				return
			}
		}

		if len(resp.LatestReplayId) > 0 {
			s.mu.Lock()
			s.lastReplayID = resp.LatestReplayId
			s.mu.Unlock()
		}

		if resp.PendingNumRequested == 0 {
			flowReq := &FetchRequest{
				TopicName:    s.config.TopicName,
				NumRequested: s.config.BatchSize,
			}
			if err := s.stream.Send(flowReq); err != nil {
				s.client.log.Errorf("send flow control FetchRequest (topic=%s): %v", s.config.TopicName, err)
				s.lastError.Store(err)
				s.lastErrorTime.Store(time.Now().UnixNano())

				if reconnErr := s.reconnectWithBackoff(ctx); reconnErr != nil {
					s.mu.Lock()
					s.streamErr = reconnErr
					s.state = StreamStateDisconnected
					s.mu.Unlock()
				}
				return
			}
		}
	}
}

// reconnectWithBackoff attempts to re-establish the subscribe stream using
// exponential backoff with jitter. On Unauthenticated errors it invokes the
// parent client's auth-refresh callback before retrying.
func (s *Subscription) reconnectWithBackoff(ctx context.Context) error {
	s.mu.Lock()
	if s.state == StreamStateClosing {
		s.mu.Unlock()
		return errors.New("subscription is closing, aborting reconnect")
	}
	s.state = StreamStateReconnecting
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()

	s.reconnectCount.Add(1)

	for attempt := 0; ; attempt++ {
		if s.client.maxReconnect > 0 && attempt >= s.client.maxReconnect {
			return fmt.Errorf("max reconnect attempts (%d) exceeded", s.client.maxReconnect)
		}

		delay := grpcBackoffWithJitter(s.client.baseBackoff, s.client.maxBackoff, attempt)
		s.client.log.Infof("Reconnecting topic=%s in %v (attempt %d)...", s.config.TopicName, delay, attempt+1)

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}

		// If the last error was Unauthenticated, ask the client to refresh auth.
		if lastErr, ok := s.lastError.Load().(error); ok && lastErr != nil {
			if grpcErr, isGRPC := status.FromError(lastErr); isGRPC {
				switch grpcErr.Code() {
				case codes.Unauthenticated:
					if err := s.client.refreshAuth(ctx); err != nil {
						s.client.log.Warnf("Auth refresh failed: %v", err)
					}
				case codes.PermissionDenied:
					s.client.log.Errorf(
						"Pub/Sub PermissionDenied on topic %q — check that the Salesforce user has the 'Subscribe to Platform Events' or CDC permission and that the topic name is correct",
						s.config.TopicName,
					)
				}
			}
		}

		s.mu.Lock()
		if s.state == StreamStateClosing {
			s.mu.Unlock()
			return errors.New("subscription is closing, aborting reconnect")
		}
		err := s.connectLocked(ctx)
		s.mu.Unlock()

		if err == nil {
			s.client.log.Infof("Reconnected successfully on attempt %d (topic=%s)", attempt+1, s.config.TopicName)
			return nil
		}
		s.client.log.Warnf("Reconnect attempt %d failed (topic=%s): %v", attempt+1, s.config.TopicName, err)
		s.lastError.Store(err)
		s.lastErrorTime.Store(time.Now().UnixNano())
	}
}

// markReadyLocked schedules the ready channel to be closed after the subscribe
// settle delay. Caller must hold s.mu and have set s.done before calling.
func (s *Subscription) markReadyLocked() {
	if s.ready == nil {
		return
	}
	select {
	case <-s.ready:
		return // already scheduled or closed
	default:
	}
	ch := s.ready
	done := s.done
	go func() {
		timer := time.NewTimer(subscribeSettleDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-done:
			return
		}
		select {
		case <-ch:
		default:
			close(ch)
		}
	}()
}

// Events returns the buffered channel of decoded Pub/Sub events. The channel
// is owned by the subscription and closed via Close; callers should not close
// it.
func (s *Subscription) Events() <-chan *PubSubEvent {
	return s.eventBuffer
}

// WaitReady blocks until the subscription is considered established (see
// subscribeSettleDelay) or ctx is cancelled.
func (s *Subscription) WaitReady(ctx context.Context) error {
	s.mu.Lock()
	ch := s.ready
	s.mu.Unlock()
	if ch == nil {
		return errors.New("subscription not connected")
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StreamErr returns the current stream error, if any.
func (s *Subscription) StreamErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamErr
}

// FetchBatch drains buffered events up to maxEvents and returns them along
// with the latest replay ID seen.
func (s *Subscription) FetchBatch(maxEvents int) ([]*PubSubEvent, []byte, error) {
	s.mu.Lock()
	streamErr := s.streamErr
	s.mu.Unlock()

	if streamErr != nil {
		return nil, nil, fmt.Errorf("Pub/Sub stream error: %w", streamErr)
	}

	var events []*PubSubEvent
	var latestReplayID []byte

	for range maxEvents {
		select {
		case evt := <-s.eventBuffer:
			events = append(events, evt)
			latestReplayID = evt.ReplayID
		default:
			goto done
		}
	}
done:

	if latestReplayID == nil {
		s.mu.Lock()
		latestReplayID = s.lastReplayID
		s.mu.Unlock()
	}
	return events, latestReplayID, nil
}

// DrainBuffer returns all remaining events from the buffer without blocking.
func (s *Subscription) DrainBuffer() []*PubSubEvent {
	var events []*PubSubEvent
	for {
		select {
		case evt := <-s.eventBuffer:
			events = append(events, evt)
		default:
			return events
		}
	}
}

// Health returns a point-in-time snapshot of the subscription's health.
func (s *Subscription) Health() SubscriptionHealth {
	s.mu.Lock()
	st := s.state
	topic := s.config.TopicName
	s.mu.Unlock()

	h := SubscriptionHealth{
		State:              st,
		TopicName:          topic,
		EventsReceived:     s.eventsReceived.Load(),
		EventsDropped:      s.eventsDropped.Load(),
		EventsDecodeErrors: s.eventsDecodeErrors.Load(),
		ReconnectCount:     s.reconnectCount.Load(),
	}
	if t := s.lastEventTime.Load(); t > 0 {
		h.LastEventTime = time.Unix(0, t)
	}
	if t := s.lastErrorTime.Load(); t > 0 {
		h.LastErrorTime = time.Unix(0, t)
	}
	if e, ok := s.lastError.Load().(error); ok {
		h.LastError = e
	}

	bufLen := len(s.eventBuffer)
	bufCap := cap(s.eventBuffer)
	if bufCap > 0 {
		h.BufferUtilization = float64(bufLen) / float64(bufCap)
	}
	return h
}

// Close shuts down the subscription, cancels its receive loop, and removes it
// from its parent client. The underlying gRPC connection is owned by the
// client and is not closed here.
func (s *Subscription) Close() error {
	return s.closeWithTimeout(0)
}

// CloseWithTimeout closes the subscription. If timeout > 0 it waits up to that
// duration for the receive loop to exit before returning.
func (s *Subscription) CloseWithTimeout(timeout time.Duration) error {
	return s.closeWithTimeout(timeout)
}

func (s *Subscription) closeWithTimeout(timeout time.Duration) error {
	s.mu.Lock()
	s.state = StreamStateClosing
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		if timeout > 0 {
			t := time.NewTimer(timeout)
			select {
			case <-done:
			case <-t.C:
				s.client.log.Warnf("Timed out waiting for receive loop to exit (topic=%s)", s.config.TopicName)
			}
			t.Stop()
		} else {
			<-done
		}
	}

	s.mu.Lock()
	s.state = StreamStateDisconnected
	s.mu.Unlock()

	s.client.removeSubscription(s)
	return nil
}

// isCDCTopic returns true if the topic path matches the CDC convention
// (/data/...ChangeEvent or /data/ChangeEvents).
func isCDCTopic(topic string) bool {
	return strings.HasPrefix(topic, "/data/") &&
		(strings.HasSuffix(topic, "ChangeEvent") || strings.HasSuffix(topic, "ChangeEvents"))
}

// extractCDCFields populates CDC-specific fields on the event from the decoded
// Avro payload.
func extractCDCFields(event *PubSubEvent, decoded map[string]any) {
	defer func() {
		if event.Fields == nil {
			event.Fields = decoded
		}
	}()

	headerMap, ok := decoded["ChangeEventHeader"].(map[string]any)
	if !ok {
		return
	}

	if ct, ok := UnwrapAvroString(headerMap["changeType"]); ok {
		event.ChangeType = ct
	}
	if en, ok := headerMap["entityName"].(string); ok {
		event.EntityName = en
	}
	if arr, ok := UnwrapAvroArray(headerMap["recordIds"]); ok {
		for _, id := range arr {
			if s, ok := id.(string); ok {
				event.RecordIDs = append(event.RecordIDs, s)
			}
		}
	}
	if arr, ok := UnwrapAvroArray(headerMap["changedFields"]); ok {
		fields := make(map[string]any, len(arr))
		for _, f := range arr {
			s, ok := f.(string)
			if !ok {
				continue
			}
			if val, exists := decoded[s]; exists {
				fields[s] = val
			}
		}
		if len(fields) > 0 {
			event.Fields = fields
		}
	}
}
