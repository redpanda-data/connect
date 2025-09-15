package http_helper

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Helper: create a basic GET request to a given URL with context
func newReq(ctx context.Context, url string) *http.Request {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	return req
}

func TestBackoffWithJitter_BoundsAndCap(t *testing.T) {
	t.Parallel()

	base := 10 * time.Millisecond
	duration := 50 * time.Millisecond

	for attempt := 0; attempt < 6; attempt++ {
		d := backoffWithJitter(base, duration, attempt)

		// expected, before jitter and cap
		expected := base << attempt
		if expected > duration {
			expected = duration
		}

		// jitter result must be within [expected/2, 3*expected/2)
		minJ := expected / 2
		maxJ := expected + expected/2

		if d < minJ || d >= maxJ {
			t.Fatalf(`attempt=%d backoff out of bounds: got %v; want [%v, %v)`, attempt, d, minJ, maxJ)
		}
	}
}

func TestDoRequestWithRetries_AuthErrors401And403(t *testing.T) {
	t.Parallel()

	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "nope", code)
			}))
			defer srv.Close()

			ctx := t.Context()
			req := newReq(ctx, srv.URL)
			resp, err := DoRequestWithRetries(ctx, srv.Client(), req, RetryOptions{})

			if resp != nil || err == nil {
				t.Fatalf("expected error for %d; got resp=%v err=%v", code, resp, err)
			}
			var he *HTTPError
			if !errors.As(err, &he) {
				t.Fatalf("expected HTTPError, got %T", err)
			}
			if he.StatusCode != code {
				t.Fatalf("status = %d; want %d", he.StatusCode, code)
			}
			if !IsAuthError(err) {
				t.Fatalf("IsAuthError = false; want true")
			}
		})
	}
}

func TestDoRequestWithRetries_200WithAuthHeaderPolicySignalsError(t *testing.T) {
	t.Parallel()

	const hdr = "X-Seraph-LoginReason"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(hdr, "AUTHENTICATED_FAILED")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok but not really"))
	}))
	defer srv.Close()

	policy := &AuthHeaderPolicy{
		HeaderName: hdr,
		IsProblem: func(val string) bool {
			return strings.Contains(val, "AUTH")
		},
	}

	ctx := t.Context()
	req := newReq(ctx, srv.URL)
	resp, err := DoRequestWithRetries(ctx, srv.Client(), req, RetryOptions{AuthHeaderPolicy: policy})

	if resp != nil || err == nil {
		t.Fatalf("expected auth header error; got resp=%v err=%v", resp, err)
	}
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected HTTPError, got %T", err)
	}
	if he.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", he.StatusCode)
	}
	if !IsAuthError(err) {
		t.Fatalf("IsAuthError = false; want true")
	}
}

func TestDoRequestWithRetries_429WithRetryAfterHonoredAndExhausts(t *testing.T) {
	t.Parallel()

	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Retry-After", "0") // no real wait
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx := t.Context()
	req := newReq(ctx, srv.URL)
	opts := RetryOptions{
		MaxRetries: 2, // expect total hits = MaxRetries + 1
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   5 * time.Millisecond,
	}
	resp, err := DoRequestWithRetries(ctx, srv.Client(), req, opts)

	if resp != nil || err == nil {
		t.Fatalf("expected 429 error after retries; got resp=%v err=%v", resp, err)
	}
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected HTTPError 429; got %T %v", err, err)
	}
	if hits != opts.MaxRetries+1 {
		t.Fatalf("server hits = %d; want %d", hits, opts.MaxRetries+1)
	}
}

func TestDoRequestWithRetries_429BackoffCanceledByContext(t *testing.T) {
	t.Parallel()

	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		// No Retry-After, so code will use backoff > 0 and wait
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	// Use a short timeout to cancel during the backoff wait
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()

	req := newReq(ctx, srv.URL)
	opts := RetryOptions{
		MaxRetries: 5,
		BaseDelay:  50 * time.Millisecond, // ensure backoff > ctx timeout
		MaxDelay:   100 * time.Millisecond,
	}
	resp, err := DoRequestWithRetries(ctx, srv.Client(), req, opts)

	if resp != nil || err == nil {
		t.Fatalf("expected context cancellation; got resp=%v err=%v", resp, err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled; got %v", err)
	}
	// Usually one hit (first 429), then canceled during the wait.
	if hits < 1 {
		t.Fatalf("server hits = %d; want >= 1", hits)
	}
}

func TestDoRequestWithRetries_OtherNon2xxAsHTTPError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx := t.Context()
	req := newReq(ctx, srv.URL)
	resp, err := DoRequestWithRetries(ctx, srv.Client(), req, RetryOptions{})

	if resp != nil || err == nil {
		t.Fatalf("expected non-2xx error; got resp=%v err=%v", resp, err)
	}
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected HTTPError; got %T", err)
	}
	if he.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500", he.StatusCode)
	}
	if IsAuthError(err) {
		t.Fatalf("IsAuthError = true; want false")
	}
}

func TestDoRequestWithRetries_Success200(t *testing.T) {
	t.Parallel()

	const body = "hello world"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	ctx := t.Context()
	req := newReq(ctx, srv.URL)
	resp, err := DoRequestWithRetries(ctx, srv.Client(), req, RetryOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}(resp.Body)

	got, _ := io.ReadAll(resp.Body)
	if string(got) != body {
		t.Fatalf("body = %q; want %q", string(got), body)
	}
}

func TestIsAuthError_ReasonContainsAuth(t *testing.T) {
	t.Parallel()

	err := &HTTPError{
		StatusCode: http.StatusOK,
		Reason:     "auth/login issue indicated by header",
	}
	if !IsAuthError(err) {
		t.Fatalf("IsAuthError = false; want true for auth-like reason")
	}
}

func TestIsAuthError_NonAuthHTTPError(t *testing.T) {
	t.Parallel()

	err := &HTTPError{
		StatusCode: http.StatusBadRequest,
		Reason:     "Bad Request",
	}
	if IsAuthError(err) {
		t.Fatalf("IsAuthError = true; want false for non-auth error")
	}
}
