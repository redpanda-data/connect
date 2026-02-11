// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http_metrics

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

	// in-flight ++
	atomic.AddInt64(&t.inflightVal, 1)
	t.inflight.Set(atomic.LoadInt64(&t.inflightVal))

	// always record end-of-request updates
	defer func() {
		// duration in nanoseconds (MetricTimer expects int64)
		t.duration.Timing(time.Since(start).Nanoseconds())
		// in-flight --
		atomic.AddInt64(&t.inflightVal, -1)
		t.inflight.Set(atomic.LoadInt64(&t.inflightVal))
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
		client = &http.Client{}
	}
	clone := *client
	clone.Transport = NewTransport(m, namespace, client.Transport)
	return &clone
}
