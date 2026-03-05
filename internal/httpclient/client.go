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

package httpclient

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// NewClient assembles an *http.Client from a Config and Resources.
//
// The RoundTripper chain from outermost to innermost:
//   - Tracing
//   - Max response body limit
//   - Retry
//   - TPS Rate Limit
//   - Metrics
//   - Logging
//   - Auth
//   - Base Transport
func NewClient(cfg Config, res *service.Resources) (*http.Client, error) {
	if res == nil {
		panic("httpclient: NewClient called with nil Resources")
	}

	// 1. Base transport (TCP dialer, proxy, TLS, HTTP/2).
	inner, err := newBaseTransport(cfg)
	if err != nil {
		return nil, err
	}

	// 2. Auth layer (product-supplied via Config.AuthSigner).
	rt := newAuthTransport(inner, cfg, res.FS())

	// 3. Logging (if configured).
	rt = newLoggingTransport(rt, res.Logger(), cfg.AccessLogLevel, cfg.AccessLogBodyLimit)

	// 4. Metrics.
	rt = newMetricsTransport(rt, newClientMetrics(res.Metrics(), cfg.MetricPrefix))

	// 5. TPS rate limit (if configured).
	rt = newTPSTransport(rt, cfg.TPSLimit, cfg.TPSBurst)

	// 6. Retry (always present: adaptive 429 at minimum).
	rt = newRetryTransport(rt, cfg, cfg.Retry, res.Logger())

	// 7. Max response body limit.
	rt = newMaxBodyTransport(rt, cfg.Transport.MaxResponseBodyBytes)

	// 8. Tracing (outermost).
	rt = newTracingTransport(rt, res.OtelTracer())

	return &http.Client{
		Transport: rt,
		Timeout:   cfg.Timeout,
	}, nil
}

// clientMetrics holds benthos metrics for the HTTP client.
type clientMetrics struct {
	requestDuration *service.MetricTimer   // labels: method, code
	requestCount    *service.MetricCounter // labels: method, code
	requestErrors   *service.MetricCounter // labels: method
	activeRequests  *service.MetricGauge
}

// newClientMetrics creates a clientMetrics from a benthos Metrics registry.
// Returns nil if prefix is empty, disabling metrics.
func newClientMetrics(m *service.Metrics, prefix string) *clientMetrics {
	if prefix == "" {
		return nil
	}
	return &clientMetrics{
		requestDuration: m.NewTimer(prefix+"_request_duration", "method", "code"),
		requestCount:    m.NewCounter(prefix+"_request_total", "method", "code"),
		requestErrors:   m.NewCounter(prefix+"_request_errors", "method"),
		activeRequests:  m.NewGauge(prefix + "_request_active"),
	}
}

// ErrUnexpectedResp is returned when an HTTP request returned an unexpected
// response code.
type ErrUnexpectedResp struct {
	Code   int
	Status string
	Body   []byte
}

// Error returns the error string.
func (e ErrUnexpectedResp) Error() string {
	body := strings.ReplaceAll(string(e.Body), "\n", "")
	return fmt.Sprintf("HTTP request returned unexpected response code (%d): %s, body: %s", e.Code, e.Status, body)
}
