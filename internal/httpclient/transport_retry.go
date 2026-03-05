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
	"context"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// retryTransport implements retry with exponential backoff and jitter.
//
// When no RetryConfig is provided, it operates in adaptive 429 mode: only
// retries 429 responses using field-configured backoff settings.
//
// When a RetryConfig IS provided, it governs all retry behavior including which
// status codes to retry on, drop on, and treat as successful.
type retryTransport struct {
	inner http.RoundTripper

	// Retry configuration: either from RetryConfig or adaptive 429 defaults.
	maxRetries      int
	retryStatuses   []int // sorted
	dropStatuses    []int // sorted
	successStatuses []int // sorted
	initialInterval time.Duration
	maxInterval     time.Duration

	log *service.Logger
}

func (*retryTransport) contains(sorted []int, v int) bool {
	_, ok := slices.BinarySearch(sorted, v)
	return ok
}

var _ http.RoundTripper = (*retryTransport)(nil)

func newRetryTransport(inner http.RoundTripper, cfg Config, rc *RetryConfig, log *service.Logger) http.RoundTripper {
	rt := &retryTransport{
		inner: inner,
		log:   log,
	}

	if rc != nil {
		// Full retry mode from Go API.
		rc.normalize()
		rt.maxRetries = rc.MaxRetries
		rt.retryStatuses = rc.RetryStatuses
		rt.dropStatuses = rc.DropStatuses
		rt.successStatuses = rc.SuccessStatuses
		rt.initialInterval = rc.InitialInterval
		rt.maxInterval = rc.MaxInterval

		// Fall back to field-configured timing if RetryConfig intervals are zero.
		if rt.initialInterval == 0 {
			rt.initialInterval = cfg.BackoffInitialInterval
		}
		if rt.maxInterval == 0 {
			rt.maxInterval = cfg.BackoffMaxInterval
		}
	} else {
		// Adaptive 429-only mode from field config.
		rt.maxRetries = cfg.BackoffMaxRetries
		rt.retryStatuses = []int{429}
		rt.dropStatuses = nil
		rt.successStatuses = nil
		rt.initialInterval = cfg.BackoffInitialInterval
		rt.maxInterval = cfg.BackoffMaxInterval
	}

	// Ensure sane defaults.
	if rt.maxRetries <= 0 {
		rt.maxRetries = 3
	}
	if rt.initialInterval <= 0 {
		rt.initialInterval = time.Second
	}
	if rt.maxInterval <= 0 {
		rt.maxInterval = 30 * time.Second
	}

	return rt
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Warn if body is present but GetBody is nil (can't replay on retry).
	if req.Body != nil && req.GetBody == nil {
		if t.log != nil {
			t.log.Warn("HTTP request has body but no GetBody; retries will be skipped")
		}
	}

	span := trace.SpanFromContext(req.Context())

	var (
		resp *http.Response
		err  error
	)

	for attempt := 0; attempt <= t.maxRetries; attempt++ {
		if attempt > 0 {
			// Restore body for retry.
			if req.GetBody != nil {
				if req.Body, err = req.GetBody(); err != nil {
					return nil, err
				}
			} else if req.Body != nil {
				// Can't replay body, return last response/error.
				return resp, err
			}
		}

		resp, err = t.inner.RoundTrip(req)
		if err != nil {
			// Network error: record event and retry.
			if attempt < t.maxRetries {
				span.AddEvent("http.retry", trace.WithAttributes(
					attribute.Int("http.request.resend_count", attempt+1),
					attribute.String("error.type", err.Error()),
				))
				if waitErr := t.backoff(req.Context(), attempt, nil); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			return nil, err
		}

		// Check status code classification.
		code := resp.StatusCode
		if t.contains(t.successStatuses, code) {
			return resp, nil
		}
		if t.contains(t.dropStatuses, code) {
			return resp, nil
		}
		if t.contains(t.retryStatuses, code) {
			if attempt < t.maxRetries {
				span.AddEvent("http.retry", trace.WithAttributes(
					attribute.Int("http.request.resend_count", attempt+1),
					attribute.Int("http.response.status_code", code),
				))
				// Drain body before retry.
				drainBody(resp)
				if berr := t.backoff(req.Context(), attempt, resp); berr != nil {
					return nil, berr
				}
				continue
			}
			return resp, nil
		}

		// Not in any classification set: return as-is.
		return resp, nil
	}

	return resp, err
}

// backoff sleeps using exponential backoff with jitter. If the response
// contains a Retry-After header (for 429), it is respected.
func (t *retryTransport) backoff(ctx context.Context, attempt int, resp *http.Response) error {
	delay := t.calculateBackoff(attempt)

	// Respect Retry-After header if present, capped to maxInterval to prevent
	// a malicious server from stalling the client indefinitely.
	if resp != nil {
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				raDelay := min(time.Duration(secs)*time.Second, t.maxInterval)
				if raDelay > delay {
					delay = raDelay
				}
			}
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// calculateBackoff returns the backoff duration for a given attempt using
// exponential backoff with jitter: min(inner * 2^attempt, max) + jitter
// where jitter is random in [-delay/2, +delay/2].
func (t *retryTransport) calculateBackoff(attempt int) time.Duration {
	inner := float64(t.initialInterval)
	delay := inner * math.Pow(2, float64(attempt))
	maxDelay := float64(t.maxInterval)
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter: [-delay/2, +delay/2].
	jitter := (rand.Float64() - 0.5) * delay
	delay += jitter

	if delay < 0 {
		delay = 0
	}
	return time.Duration(delay)
}

// drainBody reads and closes the response body to allow connection reuse.
// Reads at most 1MB to avoid stalling on unexpectedly large error bodies.
func drainBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}
}
