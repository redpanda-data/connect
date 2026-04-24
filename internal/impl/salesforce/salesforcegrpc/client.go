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
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	salesforcePubSubEndpoint = "api.pubsub.salesforce.com:443"

	// subscribeSettleDelay is how long after a successful subscribe Send we
	// wait before signalling readiness. Salesforce acknowledges the stream
	// immediately at the gRPC layer, but routing the subscription internally
	// takes additional time; events published in that window are dropped when
	// using ReplayPreset_LATEST. Empirically 3s is sufficient on the Pub/Sub
	// API; 5s provides safety margin without meaningfully slowing startup.
	subscribeSettleDelay = 5 * time.Second
)

// Config holds optional configuration for a gRPC Pub/Sub client.
type Config struct {
	// BaseBackoff is the initial reconnection delay (default 500ms).
	BaseBackoff time.Duration
	// MaxBackoff caps the reconnection delay (default 30s).
	MaxBackoff time.Duration
	// MaxReconnect limits reconnection attempts (0 = unlimited).
	MaxReconnect int
	// Metrics attaches a Benthos metrics handle for counters and gauges.
	Metrics *service.Metrics
	// OnAuthRefresh is called on Unauthenticated errors during reconnection.
	// It receives the reconnection context and should return fresh credentials.
	OnAuthRefresh func(ctx context.Context) (token, instanceURL, tenantID string, err error)
}

// Client manages a gRPC connection to the Salesforce Pub/Sub API.
type Client struct {
	conn        *grpc.ClientConn
	pubsub      PubSubClient
	log         *service.Logger
	schemaCache *SchemaCache

	mu          sync.Mutex
	bearerToken string
	instanceURL string
	tenantID    string

	stream       PubSub_SubscribeClient
	config       SubscriptionConfig
	lastReplayID []byte
	eventBuffer  chan *PubSubEvent
	cancel       context.CancelFunc
	done         chan struct{}
	ready        chan struct{}
	streamErr    error
	state        StreamState

	// Backoff configuration
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	maxReconnect int // 0 = unlimited

	// Auth refresh callback (called on Unauthenticated errors during reconnect)
	onAuthRefresh func(ctx context.Context) (token, url, tenantID string, err error)

	// Optional metrics
	metrics *service.Metrics

	// Atomic counters for health reporting
	eventsReceived     atomic.Int64
	eventsDropped      atomic.Int64
	eventsDecodeErrors atomic.Int64
	reconnectCount     atomic.Int64
	lastEventTime      atomic.Int64 // unix nano
	lastError          atomic.Value // stores error
	lastErrorTime      atomic.Int64 // unix nano
}

// NewClient creates a new gRPC client connected to the Salesforce Pub/Sub API.
// sub specifies the topic, batch size, and buffer size.
// cfg provides optional backoff, metrics, and auth refresh configuration.
func NewClient(
	log *service.Logger,
	instanceURL, tenantID, bearerToken string,
	sub SubscriptionConfig,
	cfg Config,
) (*Client, error) {
	tlsCreds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})

	conn, err := grpc.NewClient(
		salesforcePubSubEndpoint,
		grpc.WithTransportCredentials(tlsCreds),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to Salesforce Pub/Sub API: %w", err)
	}

	pubsubClient := NewPubSubClient(conn)

	baseBackoff := cfg.BaseBackoff
	if baseBackoff <= 0 {
		baseBackoff = 500 * time.Millisecond
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	return &Client{
		conn:          conn,
		pubsub:        pubsubClient,
		log:           log,
		bearerToken:   bearerToken,
		instanceURL:   instanceURL,
		tenantID:      tenantID,
		config:        sub,
		eventBuffer:   make(chan *PubSubEvent, sub.BufferSize),
		done:          make(chan struct{}),
		ready:         make(chan struct{}),
		schemaCache:   NewSchemaCache(pubsubClient, bearerToken, instanceURL, tenantID),
		state:         StreamStateDisconnected,
		baseBackoff:   baseBackoff,
		maxBackoff:    maxBackoff,
		maxReconnect:  cfg.MaxReconnect,
		onAuthRefresh: cfg.OnAuthRefresh,
		metrics:       cfg.Metrics,
	}, nil
}

// Connect starts the subscribe stream and background goroutine to receive events.
// If replayID is non-nil, subscription resumes after that point (CUSTOM preset).
// Otherwise, the configured preset (LATEST or EARLIEST) is used.
func (c *Client) Connect(ctx context.Context, replayPreset ReplayPreset, replayID []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.config.ReplayPreset = replayPreset
	if len(replayID) > 0 {
		c.config.ReplayID = replayID
		c.lastReplayID = replayID
	}

	return c.connectLocked(ctx)
}

// connectLocked opens a new stream and starts the receive loop.
// Must be called with c.mu held.
func (c *Client) connectLocked(ctx context.Context) error {
	c.state = StreamStateConnecting
	c.streamErr = nil

	streamCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	md := metadata.Pairs(
		"accesstoken", c.bearerToken,
		"instanceurl", c.instanceURL,
		"tenantid", c.tenantID,
	)
	streamCtx = metadata.NewOutgoingContext(streamCtx, md)

	stream, err := c.pubsub.Subscribe(streamCtx)
	if err != nil {
		cancel()
		c.state = StreamStateDisconnected
		return fmt.Errorf("open subscribe stream: %w", err)
	}
	c.stream = stream

	fetchReq := &FetchRequest{
		TopicName:    c.config.TopicName,
		NumRequested: c.config.BatchSize,
	}

	if len(c.lastReplayID) > 0 {
		fetchReq.ReplayPreset = ReplayPreset_CUSTOM
		fetchReq.ReplayId = c.lastReplayID
	} else {
		fetchReq.ReplayPreset = c.config.ReplayPreset
	}

	if err := stream.Send(fetchReq); err != nil {
		cancel()
		c.state = StreamStateDisconnected
		return fmt.Errorf("send initial FetchRequest: %w", err)
	}

	c.log.Infof("Pub/Sub subscription started on topic %s (preset=%v)", c.config.TopicName, fetchReq.ReplayPreset)
	c.state = StreamStateConnected

	// Subscription is established once the server accepts the initial
	// FetchRequest. Salesforce does not send a response until an event arrives,
	// so waiting for the first Recv would block indefinitely on idle topics.
	c.markReadyLocked()

	c.done = make(chan struct{})
	go c.receiveLoop(ctx)

	return nil
}

// isCDCTopic returns true if the topic path matches the CDC convention
// (/data/...ChangeEvent or /data/ChangeEvents).
func isCDCTopic(topic string) bool {
	return strings.HasPrefix(topic, "/data/") &&
		(strings.HasSuffix(topic, "ChangeEvent") || strings.HasSuffix(topic, "ChangeEvents"))
}

// receiveLoop reads from the gRPC stream and pushes decoded events into the buffer.
// On stream errors it attempts reconnection with backoff instead of exiting.
func (c *Client) receiveLoop(ctx context.Context) {
	// Capture done at goroutine start. reconnectWithBackoff → connectLocked replaces
	// c.done with a fresh channel for the new goroutine; closing the old reference
	// here prevents a double-close panic when both goroutines eventually return.
	done := c.done
	defer close(done)

	for {
		resp, err := c.stream.Recv()
		if err != nil {
			// Check if we're shutting down
			c.mu.Lock()
			if c.state == StreamStateClosing {
				c.streamErr = err
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()

			c.log.Errorf("Pub/Sub stream error: %v", err)
			c.lastError.Store(err)
			c.lastErrorTime.Store(time.Now().UnixNano())

			if reconnErr := c.reconnectWithBackoff(ctx); reconnErr != nil {
				c.mu.Lock()
				c.streamErr = reconnErr
				c.state = StreamStateDisconnected
				c.mu.Unlock()
				c.log.Errorf("Reconnection failed permanently: %v", reconnErr)
			}
			return
		}

		if len(resp.Events) == 0 {
			if len(resp.LatestReplayId) > 0 {
				c.mu.Lock()
				c.lastReplayID = resp.LatestReplayId
				c.mu.Unlock()
			}
			continue
		}

		isCDC := isCDCTopic(c.config.TopicName)

		for _, consumerEvent := range resp.Events {
			event := consumerEvent.Event
			if event == nil {
				continue
			}

			schema, err := c.schemaCache.GetSchema(ctx, event.SchemaId)
			if err != nil {
				c.log.Errorf("get schema for event (schemaID=%s): %v", event.SchemaId, err)
				c.eventsDecodeErrors.Add(1)
				continue
			}

			decoded, err := DecodeAvroPayload(schema, event.Payload)
			if err != nil {
				c.log.Errorf("decode Avro payload (schemaID=%s): %v", event.SchemaId, err)
				c.eventsDecodeErrors.Add(1)
				continue
			}

			pubsubEvent := &PubSubEvent{
				ReplayID:   consumerEvent.ReplayId,
				SchemaID:   event.SchemaId,
				TopicName:  c.config.TopicName,
				RawPayload: decoded,
			}

			if isCDC {
				pubsubEvent.Type = EventTypeCDC
				extractCDCFields(pubsubEvent, decoded)
			} else {
				pubsubEvent.Type = EventTypePlatform
				pubsubEvent.Fields = decoded
			}

			select {
			case c.eventBuffer <- pubsubEvent:
				c.eventsReceived.Add(1)
				c.lastEventTime.Store(time.Now().UnixNano())
				c.log.Debugf("Pub/Sub event received (topic=%s, schemaID=%s, replayID=%x)", pubsubEvent.TopicName, pubsubEvent.SchemaID, pubsubEvent.ReplayID)
			default:
				c.eventsDropped.Add(1)
				c.log.Warn("Pub/Sub event buffer full, dropping event")
			}
		}

		if len(resp.LatestReplayId) > 0 {
			c.mu.Lock()
			c.lastReplayID = resp.LatestReplayId
			c.mu.Unlock()
		}

		if resp.PendingNumRequested == 0 {
			flowReq := &FetchRequest{
				TopicName:    c.config.TopicName,
				NumRequested: c.config.BatchSize,
			}
			if err := c.stream.Send(flowReq); err != nil {
				c.log.Errorf("send flow control FetchRequest: %v", err)
				c.lastError.Store(err)
				c.lastErrorTime.Store(time.Now().UnixNano())

				if reconnErr := c.reconnectWithBackoff(ctx); reconnErr != nil {
					c.mu.Lock()
					c.streamErr = reconnErr
					c.state = StreamStateDisconnected
					c.mu.Unlock()
				}
				return
			}
		}
	}
}

// grpcBackoffWithJitter computes a backoff duration with jitter for the given attempt.
func grpcBackoffWithJitter(base, maxDur time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	if maxDur <= 0 {
		maxDur = 30 * time.Second
	}

	// Guard against overflow: if attempt is large, just use max
	if attempt > 30 {
		return maxDur
	}

	d := base << uint(attempt)
	if d <= 0 || d > maxDur {
		d = maxDur
	}

	jitter := time.Duration(rand.Int64N(int64(d))) - d/2
	return d + jitter
}

// reconnectWithBackoff attempts to re-establish the subscription stream using
// exponential backoff with jitter. It respects ctx cancellation and
// maxReconnect limits. On Unauthenticated errors it invokes the auth refresh
// callback before retrying.
func (c *Client) reconnectWithBackoff(ctx context.Context) error {
	c.mu.Lock()
	if c.state == StreamStateClosing || c.state == StreamStateReconfiguring {
		c.mu.Unlock()
		return errors.New("client is closing or reconfiguring, aborting reconnect")
	}
	c.state = StreamStateReconnecting
	// Cancel the old stream context
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Unlock()

	c.reconnectCount.Add(1)

	for attempt := 0; ; attempt++ {
		if c.maxReconnect > 0 && attempt >= c.maxReconnect {
			return fmt.Errorf("max reconnect attempts (%d) exceeded", c.maxReconnect)
		}

		delay := grpcBackoffWithJitter(c.baseBackoff, c.maxBackoff, attempt)
		c.log.Infof("Reconnecting in %v (attempt %d)...", delay, attempt+1)

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}

		// If the last error was Unauthenticated, try refreshing auth
		if lastErr, ok := c.lastError.Load().(error); ok && lastErr != nil {
			if grpcErr, isGRPC := status.FromError(lastErr); isGRPC {
				switch grpcErr.Code() {
				case codes.Unauthenticated:
					if c.onAuthRefresh != nil {
						token, url, tid, err := c.onAuthRefresh(ctx)
						if err != nil {
							c.log.Warnf("Auth refresh failed: %v", err)
						} else {
							c.UpdateAuth(token, url, tid)
						}
					}
				case codes.PermissionDenied:
					c.log.Errorf(
						"Pub/Sub PermissionDenied on topic %q — check that the Salesforce user has the 'Subscribe to Platform Events' or CDC permission and that the topic name is correct",
						c.config.TopicName,
					)
				}
			}
		}

		c.mu.Lock()
		if c.state == StreamStateClosing {
			c.mu.Unlock()
			return errors.New("client is closing, aborting reconnect")
		}
		err := c.connectLocked(ctx)
		c.mu.Unlock()

		if err == nil {
			c.log.Infof("Reconnected successfully on attempt %d", attempt+1)
			return nil
		}
		c.log.Warnf("Reconnect attempt %d failed: %v", attempt+1, err)
		c.lastError.Store(err)
		c.lastErrorTime.Store(time.Now().UnixNano())
	}
}

// extractCDCFields populates CDC-specific fields on the event from the decoded Avro payload.
func extractCDCFields(event *PubSubEvent, decoded map[string]any) {
	if header, ok := decoded["ChangeEventHeader"]; ok {
		if headerMap, ok := header.(map[string]any); ok {
			if ct, ok := headerMap["changeType"]; ok {
				if ctMap, ok := ct.(map[string]any); ok {
					if s, ok := ctMap["string"].(string); ok {
						event.ChangeType = s
					}
				} else if s, ok := ct.(string); ok {
					event.ChangeType = s
				}
			}
			if en, ok := headerMap["entityName"]; ok {
				if s, ok := en.(string); ok {
					event.EntityName = s
				}
			}
			if rids, ok := headerMap["recordIds"]; ok {
				// unwrap Avro union: {"array": [...]}
				if m, ok := rids.(map[string]any); ok {
					rids = m["array"]
				}
				if arr, ok := rids.([]any); ok {
					for _, id := range arr {
						if s, ok := id.(string); ok {
							event.RecordIDs = append(event.RecordIDs, s)
						}
					}
				}
			}
			if cf, ok := headerMap["changedFields"]; ok {
				// unwrap Avro union: {"array": [...]}
				if m, ok := cf.(map[string]any); ok {
					cf = m["array"]
				}
				if arr, ok := cf.([]any); ok {
					fields := make(map[string]any)
					for _, f := range arr {
						if s, ok := f.(string); ok {
							if val, exists := decoded[s]; exists {
								fields[s] = val
							}
						}
					}
					if len(fields) > 0 {
						event.Fields = fields
					}
				}
			}
		}
	}

	if event.Fields == nil {
		event.Fields = decoded
	}
}

// FetchBatch drains buffered events up to maxEvents and returns them.
func (c *Client) FetchBatch(maxEvents int) ([]*PubSubEvent, []byte, error) {
	c.mu.Lock()
	streamErr := c.streamErr
	c.mu.Unlock()

	if streamErr != nil {
		return nil, nil, fmt.Errorf("Pub/Sub stream error: %w", streamErr)
	}

	var events []*PubSubEvent
	var latestReplayID []byte

	for range maxEvents {
		select {
		case evt := <-c.eventBuffer:
			events = append(events, evt)
			latestReplayID = evt.ReplayID
		default:
			goto done
		}
	}
done:

	if latestReplayID == nil {
		c.mu.Lock()
		latestReplayID = c.lastReplayID
		c.mu.Unlock()
	}

	return events, latestReplayID, nil
}

// DrainBuffer returns all remaining events from the buffer without blocking.
func (c *Client) DrainBuffer() []*PubSubEvent {
	var events []*PubSubEvent
	for {
		select {
		case evt := <-c.eventBuffer:
			events = append(events, evt)
		default:
			return events
		}
	}
}

// UpdateAuth updates the authentication credentials for the client and schema cache.
func (c *Client) UpdateAuth(bearerToken, instanceURL, tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bearerToken = bearerToken
	c.instanceURL = instanceURL
	c.tenantID = tenantID
	c.schemaCache.UpdateAuth(bearerToken, instanceURL, tenantID)
}

// StreamErr returns the current stream error, if any.
func (c *Client) StreamErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streamErr
}

// Events returns the buffered channel of decoded Pub/Sub events. The channel is
// owned by the client and closed via Close; callers should not close it.
func (c *Client) Events() <-chan *PubSubEvent {
	return c.eventBuffer
}

// WaitReady blocks until the subscription is considered established (see
// subscribeSettleDelay) or ctx is cancelled.
func (c *Client) WaitReady(ctx context.Context) error {
	c.mu.Lock()
	ch := c.ready
	c.mu.Unlock()
	if ch == nil {
		return errors.New("client not connected")
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// markReadyLocked schedules the ready channel to be closed after the subscribe
// settle delay. Caller must hold c.mu.
//
// Salesforce's Pub/Sub API accepts the initial FetchRequest quickly but takes
// additional wall-time to route the subscription server-side. Events published
// during that window are silently dropped when using ReplayPreset_LATEST.
// Delaying readiness by subscribeSettleDelay avoids this race.
func (c *Client) markReadyLocked() {
	if c.ready == nil {
		return
	}
	select {
	case <-c.ready:
		return // already scheduled or closed
	default:
	}
	ch := c.ready
	go func() {
		timer := time.NewTimer(subscribeSettleDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-c.done:
			return
		}
		select {
		case <-ch:
		default:
			close(ch)
		}
	}()
}

// Health returns a point-in-time snapshot of the subscription's health.
func (c *Client) Health() SubscriptionHealth {
	c.mu.Lock()
	st := c.state
	topic := c.config.TopicName
	c.mu.Unlock()

	h := SubscriptionHealth{
		State:              st,
		TopicName:          topic,
		EventsReceived:     c.eventsReceived.Load(),
		EventsDropped:      c.eventsDropped.Load(),
		EventsDecodeErrors: c.eventsDecodeErrors.Load(),
		ReconnectCount:     c.reconnectCount.Load(),
	}

	if t := c.lastEventTime.Load(); t > 0 {
		h.LastEventTime = time.Unix(0, t)
	}
	if t := c.lastErrorTime.Load(); t > 0 {
		h.LastErrorTime = time.Unix(0, t)
	}
	if e, ok := c.lastError.Load().(error); ok {
		h.LastError = e
	}

	bufLen := len(c.eventBuffer)
	bufCap := cap(c.eventBuffer)
	if bufCap > 0 {
		h.BufferUtilization = float64(bufLen) / float64(bufCap)
	}

	return h
}

// Reconfigure cancels the existing stream, drains the old buffer, applies the
// new SubscriptionConfig, and reconnects.
func (c *Client) Reconfigure(ctx context.Context, cfg SubscriptionConfig) error {
	c.mu.Lock()
	c.state = StreamStateReconfiguring // prevent receiveLoop from reconnecting while we reconfigure
	oldCancel := c.cancel
	oldDone := c.done
	c.mu.Unlock()

	// Shut down the old stream
	if oldCancel != nil {
		oldCancel()
	}
	if oldDone != nil {
		<-oldDone
	}

	// Drain old buffer
	c.DrainBuffer()

	c.mu.Lock()
	c.config = cfg
	c.eventBuffer = make(chan *PubSubEvent, cfg.BufferSize)
	c.streamErr = nil
	if len(cfg.ReplayID) > 0 {
		c.lastReplayID = cfg.ReplayID
	}
	err := c.connectLocked(ctx)
	c.mu.Unlock()

	return err
}

// Close shuts down the gRPC client, cancelling the background goroutine and closing the connection.
func (c *Client) Close() error {
	return c.CloseWithTimeout(0)
}

// CloseWithTimeout shuts down the gRPC client. If timeout > 0, it waits up to
// that duration for the receive loop to exit before closing the connection.
func (c *Client) CloseWithTimeout(timeout time.Duration) error {
	c.mu.Lock()
	c.state = StreamStateClosing
	cancel := c.cancel
	done := c.done
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		if timeout > 0 {
			t := time.NewTimer(timeout)
			select {
			case <-done:
			case <-t.C:
				c.log.Warn("Timed out waiting for receive loop to exit")
			}
			t.Stop()
		} else {
			<-done
		}
	}

	c.mu.Lock()
	c.state = StreamStateDisconnected
	c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
