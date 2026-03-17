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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failThenSucceedRT is a mock RoundTripper that fails the first N calls with
// a network error, then delegates to inner.
type failThenSucceedRT struct {
	inner    http.RoundTripper
	failFor  int
	attempts atomic.Int32
}

func (f *failThenSucceedRT) RoundTrip(req *http.Request) (*http.Response, error) {
	n := int(f.attempts.Add(1))
	if n <= f.failFor {
		return nil, errors.New("simulated network error")
	}
	return f.inner.RoundTrip(req)
}

// alwaysFailRT is a mock RoundTripper that always returns an error.
type alwaysFailRT struct {
	attempts atomic.Int32
}

func (f *alwaysFailRT) RoundTrip(*http.Request) (*http.Response, error) {
	f.attempts.Add(1)
	return nil, errors.New("permanent network error")
}

func TestRetryTransport503ThenSuccess(t *testing.T) {
	t.Log("Given: a server that returns 503 twice then 200")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.InitialInterval = time.Millisecond
	rc.MaxInterval = 5 * time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the request succeeds after 3 attempts")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestRetryTransport429WithRetryAfter(t *testing.T) {
	t.Log("Given: a server that returns 429 with Retry-After: 1 then 200")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		BackoffInitialInterval: time.Millisecond,
		BackoffMaxInterval:     2 * time.Second,
		BackoffMaxRetries:      3,
	}
	rt := newRetryTransport(http.DefaultTransport, cfg, nil, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	start := time.Now()
	resp, err := rt.RoundTrip(req)
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the retry respects the Retry-After header")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), attempts.Load())
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
}

func TestRetryTransportMaxRetriesExhausted(t *testing.T) {
	t.Log("Given: a server that always returns 503 and max retries of 2")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.MaxRetries = 2
	rc.InitialInterval = time.Millisecond
	rc.MaxInterval = 5 * time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: all retries are exhausted and the last 503 is returned")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, int32(3), attempts.Load()) // 1 initial + 2 retries
}

func TestRetryTransportContextCancelDuringBackoff(t *testing.T) {
	t.Log("Given: a server returning 503 and a very long backoff interval")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.InitialInterval = 10 * time.Second
	rc.MaxInterval = 10 * time.Second
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending a request with a 100ms timeout context")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	_, err = rt.RoundTrip(req)

	t.Log("Then: the request fails with DeadlineExceeded during backoff")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRetryTransportGetBodyNilNoRetry(t *testing.T) {
	t.Log("Given: a server returning 503 and a POST request with body but no GetBody")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.InitialInterval = time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending the request")
	body := bytes.NewReader([]byte("payload"))
	req, err := http.NewRequest(http.MethodPost, srv.URL, body)
	require.NoError(t, err)
	req.GetBody = nil
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: no retry occurs because the body cannot be replayed")
	assert.Equal(t, int32(1), attempts.Load())
}

func TestRetryTransportDropOn(t *testing.T) {
	t.Log("Given: a server returning 403 (a drop status)")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.InitialInterval = time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: no retry occurs for the drop status")
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestRetryTransportBodyReplayedOnRetry(t *testing.T) {
	t.Log("Given: a server that returns 503 once then 200, capturing request bodies")
	var bodies []string
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		n := attempts.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.InitialInterval = time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending a POST with a replayable body")
	payload := []byte("test-body")
	req, err := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(payload))
	require.NoError(t, err)
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload)), nil
	}
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the body is replayed identically on retry")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, bodies, 2)
	assert.Equal(t, "test-body", bodies[0])
	assert.Equal(t, "test-body", bodies[1])
}

func TestCalculateBackoff(t *testing.T) {
	t.Log("Given: a retryTransport with 100ms initial and 5s max interval")
	rt := &retryTransport{
		initialInterval: 100 * time.Millisecond,
		maxInterval:     5 * time.Second,
	}

	t.Log("When: calculating backoff for attempt 0 many times")
	for range 100 {
		d0 := rt.calculateBackoff(0)
		// Attempt 0: inner=100ms, jitter in [-50ms, +50ms], so [50ms, 150ms].
		assert.GreaterOrEqual(t, d0, time.Duration(0))
		assert.LessOrEqual(t, d0, 200*time.Millisecond)
	}

	t.Log("Then: higher attempts stay bounded by max interval + jitter")
	d5 := rt.calculateBackoff(5)
	assert.LessOrEqual(t, d5, 2*rt.maxInterval)
}

func TestRetryTransport429OnlyNonRetryableCode(t *testing.T) {
	t.Log("Given: a server returning 400 and adaptive 429-only retry mode")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	cfg := Config{
		BackoffInitialInterval: time.Millisecond,
		BackoffMaxInterval:     5 * time.Millisecond,
		BackoffMaxRetries:      3,
	}
	rt := newRetryTransport(http.DefaultTransport, cfg, nil, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: no retry occurs for a non-retryable status code")
	assert.Equal(t, int32(1), attempts.Load())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRetryTransport429ReadsRetryAfterSeconds(t *testing.T) {
	t.Log("Given: a server returning 429 with Retry-After: 0 then 200")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.Header().Set("Retry-After", strconv.Itoa(0))
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		BackoffInitialInterval: time.Millisecond,
		BackoffMaxInterval:     5 * time.Millisecond,
		BackoffMaxRetries:      2,
	}
	rt := newRetryTransport(http.DefaultTransport, cfg, nil, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the retry succeeds after reading Retry-After")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), attempts.Load())
}

// --- Constructor defaults ---

func TestNewRetryTransportFallbackToConfigIntervals(t *testing.T) {
	t.Log("Given: a RetryConfig with zero intervals and Config with non-zero intervals")
	rc := DefaultRetryConfig()
	rc.InitialInterval = 0
	rc.MaxInterval = 0

	cfg := Config{
		BackoffInitialInterval: 42 * time.Millisecond,
		BackoffMaxInterval:     99 * time.Millisecond,
	}

	t.Log("When: creating a retry transport")
	rt := newRetryTransport(http.DefaultTransport, cfg, rc, nil).(*retryTransport)

	t.Log("Then: it falls back to the Config interval values")
	assert.Equal(t, 42*time.Millisecond, rt.initialInterval)
	assert.Equal(t, 99*time.Millisecond, rt.maxInterval)
}

func TestNewRetryTransportSaneDefaults(t *testing.T) {
	t.Log("Given: both RetryConfig and Config have zero/negative values")
	rc := &RetryConfig{
		MaxRetries:      -1,
		InitialInterval: 0,
		MaxInterval:     0,
		RetryStatuses:   []int{500},
	}
	cfg := Config{
		BackoffInitialInterval: 0,
		BackoffMaxInterval:     0,
	}

	t.Log("When: creating a retry transport")
	rt := newRetryTransport(http.DefaultTransport, cfg, rc, nil).(*retryTransport)

	t.Log("Then: sane defaults are applied")
	assert.Equal(t, 3, rt.maxRetries)
	assert.Equal(t, time.Second, rt.initialInterval)
	assert.Equal(t, 30*time.Second, rt.maxInterval)
}

// --- RoundTrip edge cases ---

func TestRetryTransportNetworkErrorThenSuccess(t *testing.T) {
	t.Log("Given: a mock transport that fails twice with network errors then succeeds")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	mock := &failThenSucceedRT{inner: http.DefaultTransport, failFor: 2}

	rc := DefaultRetryConfig()
	rc.InitialInterval = time.Millisecond
	rc.MaxInterval = 5 * time.Millisecond
	rt := newRetryTransport(mock, Config{}, rc, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the request succeeds after retrying past the network errors")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(3), mock.attempts.Load())
}

func TestRetryTransportNetworkErrorExhausted(t *testing.T) {
	t.Log("Given: a mock transport that always fails and max retries of 2")
	mock := &alwaysFailRT{}

	rc := DefaultRetryConfig()
	rc.MaxRetries = 2
	rc.InitialInterval = time.Millisecond
	rc.MaxInterval = 5 * time.Millisecond
	rt := newRetryTransport(mock, Config{}, rc, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, "http://localhost:1", nil)
	require.NoError(t, err)
	_, err = rt.RoundTrip(req)

	t.Log("Then: the last network error is returned after exhausting retries")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permanent network error")
	assert.Equal(t, int32(3), mock.attempts.Load())
}

func TestRetryTransportSuccessStatuses(t *testing.T) {
	t.Log("Given: a server returning 201 and a retry config with 201 as a success status")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.SuccessStatuses = []int{200, 201, 202}
	rc.InitialInterval = time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodPost, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: no retry occurs for the success status")
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestRetryTransportUnclassifiedStatus(t *testing.T) {
	t.Log("Given: a server returning 418 (not in any retry/drop/success list)")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.InitialInterval = time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the response is returned as-is without retry")
	assert.Equal(t, http.StatusTeapot, resp.StatusCode)
	assert.Equal(t, int32(1), attempts.Load())
}

func TestRetryTransportGetBodyError(t *testing.T) {
	t.Log("Given: a server returning 503 and a request with a GetBody that errors")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	rc := DefaultRetryConfig()
	rc.InitialInterval = time.Millisecond
	rt := newRetryTransport(http.DefaultTransport, Config{}, rc, nil)

	t.Log("When: sending the request")
	req, err := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader([]byte("data")))
	require.NoError(t, err)
	req.GetBody = func() (io.ReadCloser, error) {
		return nil, errors.New("GetBody failed")
	}
	_, err = rt.RoundTrip(req)

	t.Log("Then: the GetBody error is propagated")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetBody failed")
	assert.Equal(t, int32(1), attempts.Load())
}

// --- Backoff edge cases ---

func TestRetryTransportRetryAfterCappedToMaxInterval(t *testing.T) {
	t.Log("Given: a server returning 429 with Retry-After: 3600 and max interval of 50ms")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		BackoffInitialInterval: time.Millisecond,
		BackoffMaxInterval:     50 * time.Millisecond,
		BackoffMaxRetries:      2,
	}
	rt := newRetryTransport(http.DefaultTransport, cfg, nil, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	start := time.Now()
	resp, err := rt.RoundTrip(req)
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the Retry-After value is capped to max interval")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), attempts.Load())
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestRetryTransportRetryAfterNonNumeric(t *testing.T) {
	t.Log("Given: a server returning 429 with a non-numeric Retry-After then 200")
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			w.Header().Set("Retry-After", "not-a-number")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		BackoffInitialInterval: time.Millisecond,
		BackoffMaxInterval:     5 * time.Millisecond,
		BackoffMaxRetries:      2,
	}
	rt := newRetryTransport(http.DefaultTransport, cfg, nil, nil)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	t.Log("Then: the non-numeric Retry-After is ignored and retry succeeds")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), attempts.Load())
}
