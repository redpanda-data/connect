// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package metrics

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Transport is a wrapper around an http.RoundTripper that tracks request metrics.
type Transport struct {
	base    http.RoundTripper
	metrics *service.Metrics
	ns      string

	inflight    *service.MetricGauge
	inflightVal int64

	total    *service.MetricCounter
	errors   *service.MetricCounter
	status   *service.MetricCounter
	duration *service.MetricTimer
}

// NewTransport creates new Transport with metrics instrumentation. It takes a metrics
// instance, namespace string for metric names, and an optional HTTP transport (if nil, creates a new one).
// The function returns Transport that tracks request metrics such as in-flight requests, response status codes, errors, and request duration.
func NewTransport(m *service.Metrics, namespace string, base http.RoundTripper) *Transport {
	if base == nil {
		base = http.DefaultTransport
	}

	return &Transport{
		base:     base,
		metrics:  m,
		ns:       namespace,
		inflight: m.NewGauge(namespace + "_in_flight"),
		total:    m.NewCounter(namespace + "_requests_total"),
		errors:   m.NewCounter(namespace + "_requests_errors"),
		status:   m.NewCounter(namespace+"_responses", "status_code"),
		duration: m.NewTimer(namespace + "_request_duration"),
	}
}

// RoundTrip implements the http.RoundTripper interface.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()

	// in-flight ++: AddInt64 returns the new value atomically, avoiding a
	// separate LoadInt64 that could observe a different value.
	t.inflight.Set(atomic.AddInt64(&t.inflightVal, 1))

	// always record end-of-request updates
	defer func() {
		// duration in nanoseconds (MetricTimer expects int64)
		t.duration.Timing(time.Since(start).Nanoseconds())
		// in-flight --
		t.inflight.Set(atomic.AddInt64(&t.inflightVal, -1))
	}()

	t.total.Incr(1)

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		t.errors.Incr(1)
		return nil, err
	}

	codeStr := strconv.Itoa(resp.StatusCode)
	t.status.Incr(1, codeStr)

	return resp, nil
}

// NewInstrumentedClient creates a new HTTP client with metrics instrumentation. It takes a metrics
// instance, namespace string for metric names, and an optional HTTP client (if nil, creates a new one).
// The function returns a clone of the input client with an instrumented transport that tracks request
// metrics such as in-flight requests, response status codes, errors, and request duration.
func NewInstrumentedClient(m *service.Metrics, namespace string, client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	clone := *client
	clone.Transport = NewTransport(m, namespace, client.Transport)
	return &clone
}
