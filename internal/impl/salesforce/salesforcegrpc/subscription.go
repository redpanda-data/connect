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

	go s.receiveLoop(ctx)

	return nil
}

// receiveLoop reads from the gRPC stream and pushes decoded events into the
// buffer. On stream errors it attempts reconnection with backoff instead of
// exiting.
func (s *Subscription) receiveLoop(ctx context.Context) {
	// Capture done at goroutine start. reconnectWithBackoff → connectLocked
	// replaces s.done with a fresh channel for the new goroutine; closing the
	// old reference here prevents a double-close panic when both goroutines
	// eventually return.
	done := s.done
	defer close(done)

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

			s.client.log.Errorf("Pub/Sub stream error (topic=%s): %v", s.config.TopicName, err)
			s.lastError.Store(err)
			s.lastErrorTime.Store(time.Now().UnixNano())

			if reconnErr := s.reconnectWithBackoff(ctx); reconnErr != nil {
				s.mu.Lock()
				s.streamErr = reconnErr
				s.state = StreamStateDisconnected
				s.mu.Unlock()
				s.client.log.Errorf("Reconnection failed permanently (topic=%s): %v", s.config.TopicName, reconnErr)
			}
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

			schema, err := s.client.schemaCache.GetSchema(ctx, event.SchemaId)
			if err != nil {
				s.client.log.Errorf("get schema for event (schemaID=%s): %v", event.SchemaId, err)
				s.eventsDecodeErrors.Add(1)
				continue
			}

			decoded, err := DecodeAvroPayload(schema, event.Payload)
			if err != nil {
				s.client.log.Errorf("decode Avro payload (schemaID=%s): %v", event.SchemaId, err)
				s.eventsDecodeErrors.Add(1)
				continue
			}

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

			select {
			case s.eventBuffer <- pubsubEvent:
				s.eventsReceived.Add(1)
				s.lastEventTime.Store(time.Now().UnixNano())
				s.client.log.Debugf("Pub/Sub event received (topic=%s, schemaID=%s, replayID=%x)", pubsubEvent.TopicName, pubsubEvent.SchemaID, pubsubEvent.ReplayID)
			default:
				s.eventsDropped.Add(1)
				s.client.log.Warnf("Pub/Sub event buffer full (topic=%s), dropping event", s.config.TopicName)
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
