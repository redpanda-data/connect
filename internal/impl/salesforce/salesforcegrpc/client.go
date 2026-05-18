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
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const salesforcePubSubEndpoint = "api.pubsub.salesforce.com:443"

// Config holds optional configuration for a gRPC Pub/Sub Client.
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

// Client wraps a single gRPC connection to the Salesforce Pub/Sub API. It
// owns the auth state and schema cache and hosts zero or more Subscriptions
// (created via Subscribe) that multiplex over the same connection.
type Client struct {
	conn        *grpc.ClientConn
	pubsub      PubSubClient
	log         *service.Logger
	schemaCache *SchemaCache

	authMu      sync.RWMutex
	bearerToken string
	instanceURL string
	tenantID    string

	// Backoff configuration shared with subscriptions.
	baseBackoff  time.Duration
	maxBackoff   time.Duration
	maxReconnect int

	// Auth refresh callback (called on Unauthenticated errors during reconnect).
	onAuthRefresh func(ctx context.Context) (token, url, tenantID string, err error)
	// authRefreshMu serialises concurrent calls into onAuthRefresh so multiple
	// subscriptions reconnecting at once don't all hit the OAuth endpoint.
	authRefreshMu sync.Mutex

	metrics *service.Metrics

	subsMu sync.Mutex
	subs   []*Subscription
}

// NewClient dials the Salesforce Pub/Sub API and returns a Client ready to host
// subscriptions.
func NewClient(
	log *service.Logger,
	instanceURL, tenantID, bearerToken string,
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
		schemaCache:   NewSchemaCache(pubsubClient, bearerToken, instanceURL, tenantID),
		baseBackoff:   baseBackoff,
		maxBackoff:    maxBackoff,
		maxReconnect:  cfg.MaxReconnect,
		onAuthRefresh: cfg.OnAuthRefresh,
		metrics:       cfg.Metrics,
	}, nil
}

// Subscribe opens a new subscribe stream for the given topic and returns a
// *Subscription bound to this client's connection.
func (c *Client) Subscribe(ctx context.Context, sub SubscriptionConfig, replayPreset ReplayPreset, replayID []byte) (*Subscription, error) {
	s := &Subscription{
		client:      c,
		config:      sub,
		isCDC:       isCDCTopic(sub.TopicName),
		eventBuffer: make(chan *PubSubEvent, sub.BufferSize),
		ready:       make(chan struct{}),
		done:        make(chan struct{}),
		state:       StreamStateDisconnected,
	}
	if err := s.connect(ctx, replayPreset, replayID); err != nil {
		return nil, err
	}
	c.subsMu.Lock()
	c.subs = append(c.subs, s)
	c.subsMu.Unlock()
	return s, nil
}

// auth returns the current auth tuple. Used by subscriptions when (re-)opening
// streams.
func (c *Client) auth() (token, instanceURL, tenantID string) {
	c.authMu.RLock()
	defer c.authMu.RUnlock()
	return c.bearerToken, c.instanceURL, c.tenantID
}

// UpdateAuth updates the authentication credentials for the client and schema
// cache. Existing subscriptions pick up the new credentials on their next
// reconnect.
func (c *Client) UpdateAuth(bearerToken, instanceURL, tenantID string) {
	c.authMu.Lock()
	c.bearerToken = bearerToken
	c.instanceURL = instanceURL
	c.tenantID = tenantID
	c.authMu.Unlock()
	c.schemaCache.UpdateAuth(bearerToken, instanceURL, tenantID)
}

// refreshAuth invokes the configured auth-refresh callback (when set) and
// applies the resulting credentials to the client. Concurrent calls are
// serialised so multiple subscriptions reconnecting simultaneously share a
// single refresh.
func (c *Client) refreshAuth(ctx context.Context) error {
	if c.onAuthRefresh == nil {
		return nil
	}
	c.authRefreshMu.Lock()
	defer c.authRefreshMu.Unlock()

	token, url, tid, err := c.onAuthRefresh(ctx)
	if err != nil {
		return err
	}
	c.UpdateAuth(token, url, tid)
	return nil
}

// Subscriptions returns the currently-active subscriptions hosted on the
// client as a fixed-cap subslice — appends by the caller will reallocate
// rather than mutate the client's internal slice. Intended for tests and
// health introspection.
func (c *Client) Subscriptions() []*Subscription {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	return c.subs[:len(c.subs):len(c.subs)]
}

// removeSubscription drops s from the client's subscription list.
func (c *Client) removeSubscription(s *Subscription) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for i, existing := range c.subs {
		if existing == s {
			c.subs = append(c.subs[:i], c.subs[i+1:]...)
			return
		}
	}
}

// Close closes all hosted subscriptions and the underlying gRPC connection.
func (c *Client) Close() error {
	return c.CloseWithTimeout(0)
}

// CloseWithTimeout closes all hosted subscriptions and the gRPC connection. If
// timeout > 0, each subscription's receive loop is given up to that duration
// to drain before the connection is torn down.
func (c *Client) CloseWithTimeout(timeout time.Duration) error {
	c.subsMu.Lock()
	subs := append([]*Subscription(nil), c.subs...)
	c.subsMu.Unlock()

	for _, s := range subs {
		if err := s.CloseWithTimeout(timeout); err != nil {
			c.log.Errorf("close subscription (topic=%s): %v", s.config.TopicName, err)
		}
	}

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// grpcBackoffWithJitter computes a backoff duration with jitter for the given
// attempt.
func grpcBackoffWithJitter(base, maxDur time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	if maxDur <= 0 {
		maxDur = 30 * time.Second
	}
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
