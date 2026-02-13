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

// salesforce_helper.go provides helpers for making HTTP requests
// with Salesforce-specific authentication and rate-limiting handling.
// It wraps HTTP responses to detect common error cases such as
// 401/403 Unauthorized, 429 Too Many Requests, and Salesforce’s
// auth-related header, which may indicate an authentication
// problem even on a 200 OK response.
//
// Overview
//  - Provides helpers for interpreting Salesforce HTTP responses with a focus on
//  authentication and rate-limiting signals.
//  - Central entry point: CheckSalesforceAuth(resp) which examines an http.Response and
//  returns a *SalesforceError for common Salesforce conditions:
//  - 401 Unauthorized: Likely invalid or expired access token.
//  - 403 Forbidden: Authenticated but insufficient permissions.
//  - 429 Too Many Requests: Salesforce is throttling; check Retry-After header.
//  - On success (no issues detected), CheckSalesforceAuth returns nil.
//  - On failure, SalesforceError includes StatusCode, Reason, Body, and Headers.
//
// When to use
//  - Immediately after getting an *http.Response from Salesforce (e.g., client.Do(req)),
//    before processing the body. This gives a consistent, structured way to react
//    to auth and rate-limit conditions.
//
// Quick Start
//
//  1. Basic request and auth check
//
//    ctx := context.Background()
//    req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://your-domain.salesforce.com/services/data/v65.0/sobjects", nil)
//    req.Header.Set("Authorization", "Bearer <access-token>")
//    req.Header.Set("Accept", "application/json")
//    req.Header.Set("User-Agent", "Redpanda-Connect")
//
//    resp, err := http.DefaultClient.Do(req)
//    if err != nil {
//    transport or network error
//    log.Fatalf("request failed: %v", err)
//    }
//    defer resp.Body.Close()
//
//     if serr := salesforce_helper.CheckSalesforceAuth(resp); serr != nil {
//     // This includes 401, 403, 429, and 200 + header-signaled problems.
//     // You can inspect the error for details.
//     if se, ok := serr.(*salesforce_helper.SalesforceError); ok {
//     log.Printf("salesforce error: status=%d reason=%s", se.StatusCode, se.Reason)
//     log.Printf("headers: %v", se.Headers)
//     }
//     return
//     }
//
//     // Safe to read/parse the response body here.
//     data, _ := io.ReadAll(resp.Body)
//     log.Printf("success: %s", string(data))
//
//  2. Handling rate limiting (429) with Retry-After
//
//     // If CheckSalesforceAuth returns a SalesforceError with StatusCode 429, look for Retry-After.
//     jerr := salesforce_helper.CheckSalesforceAuth(resp)
//     if jerr != nil {
//     if je, ok := jerr.(*salesforce_helper.SalesforceError); ok && je.StatusCode == http.StatusTooManyRequests {
//     retryAfter := je.Headers.Get("Retry-After") // integer seconds expected
//     // Convert to a duration and sleep before retrying
//     if secs, convErr := strconv.Atoi(strings.TrimSpace(retryAfter)); convErr == nil && secs >= 0 {
//     time.Sleep(time.Duration(secs) * time.Second)
//     //retry the request here
//     } else {
//     // Fallback: use a default backoff (e.g., exponential with jitter) before retry
//     }
//     }
//     }
//
//
//  3. Centralized error handling
//
//     jerr := salesforce_helper.CheckSalesforceAuth(resp)
//     if jerr != nil {
//     // Use errors.As to extract *SalesforceError
//     var je *salesforce_helper.SalesforceError
//     if errors.As(jerr, &je) {
//     switch je.StatusCode {
//     case http.StatusUnauthorized:
//     // refresh token, prompt re-login, etc.
//     case http.StatusForbidden:
//     // insufficient permissions: inform user or adjust scopes
//     case http.StatusTooManyRequests:
//     // back off and retry later
//     default:
//     // 200 with auth-related or other 4xx/5xx
//     }
//     }
//     return
//     }
//     // proceed to parse resp.Body
//
// 4. Example helper wrapping a Salesforce call
//
//     func callSalesforce(ctx context.Context, client *http.Client, req *http.Request) ([]byte, *salesforce_helper.SalesforceError) {
//     resp, err := client.Do(req.WithContext(ctx))
//     if err != nil {
//     return nil, &salesforce_helper.SalesforceError{
//     StatusCode: 0,
//     Reason: fmt.Sprintf("transport error: %v", err),
//     }
//     }
//     defer resp.Body.Close()
//
//     if jerr := salesforce_helper.CheckSalesforceAuth(resp); jerr != nil {
//     if je, ok := jerr.(*salesforce_helper.SalesforceError); ok {
//     return nil, je
//     }
//     // Wrap non-SalesforceError just in case (shouldn't happen).
//     return nil, &salesforce_helper.SalesforceError{StatusCode: resp.StatusCode, Reason: jerr.Error()}
//     }
//
//     data, readErr := io.ReadAll(resp.Body)
//     if readErr != nil {
//     return nil, &salesforce_helper.SalesforceError{StatusCode: resp.StatusCode, Reason: fmt.Sprintf("read error: %v", readErr)}
//     }
//     return data, nil
//     }
//
//	Inspecting SalesforceError
//   - On error, CheckSalesforceAuth returns *SalesforceError containing:
//   - StatusCode: HTTP status (or 200 in header-signaled cases).
//   - Reason: High-level explanation (or http.StatusText for generic 4xx/5xx).
//   - Body: Response body string (caller may truncate before logging).
//   - Headers: Cloned response headers for further inspection.
//
//   - Example:
//
// 	if jerr := salesforce_helper.CheckSalesforceAuth(resp); jerr != nil {
// 	    if je, ok := jerr.(*salesforce_helper.SalesforceError); ok {
// 	        fmt.Printf("status=%d reason=%s\n", je.StatusCode, je.Reason)
// 	    }
// 	}
//
// Best Practices
// - Always set a User-Agent to identify your application.
// - Use context timeouts or deadlines to avoid hanging requests.
// - Be mindful when logging bodies; they may contain sensitive or large content.
// - Respect Retry-After when present; otherwise choose a conservative backoff.
// - Prefer idempotent methods for automatic retries; confirm business safety for POST/PUT.
// - Consider centralizing Salesforce error handling via CheckSalesforceAuth in your HTTP layer.
//
// Dependencies
// - Standard library only: net/http, io, fmt.

package salesforcehttp

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTPError wraps non-2xx responses with useful context.
type HTTPError struct {
	StatusCode int
	Reason     string
	Body       string
	Headers    http.Header
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http error: status=%d reason=%s", e.StatusCode, e.Reason)
}

// AuthHeaderPolicy allows callers to declare a header that signals an auth problem
// even on 200 OK responses (e.g., "auth-related").
type AuthHeaderPolicy struct {
	HeaderName string                // case-insensitive
	IsProblem  func(val string) bool // return true if the header value indicates auth failure
}

// RetryOptions controls the behavior of DoRequestWithRetries.
type RetryOptions struct {
	MaxRetries       int               // retries for 429 Too Many Requests (0 = no retry)
	BaseDelay        time.Duration     // base backoff (default 500 ms)
	MaxDelay         time.Duration     // cap backoff (default 30s)
	AuthHeaderPolicy *AuthHeaderPolicy // optional header-based auth detection
}

func backoffWithJitter(base, maxDuration time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	if maxDuration <= 0 {
		maxDuration = 30 * time.Second
	}
	d := base << attempt
	if d > maxDuration {
		d = maxDuration
	}
	jitter := time.Duration(rand.Int63n(int64(d))) - d/2
	return d + jitter
}

// DoRequestWithRetries executes req, handling:
// - Auth errors on 401/403
// - Header-signaled auth problems on 200 (via AuthHeaderPolicy)
// - 429 with Retry-After or exponential backoff and jitter (up to MaxRetries)
// Other 4xx/5xx are returned as HTTPError without a retry.
func DoRequestWithRetries(
	ctx context.Context,
	client *http.Client,
	req *http.Request,
	opts RetryOptions,
) ([]byte, error) {
	if client == nil {
		client = http.DefaultClient
	}
	attempt := 0

	for {
		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			return nil, err
		}

		// 1) Explicit auth/permission failures
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			body, _ := io.ReadAll(resp.Body)
			err := resp.Body.Close()
			if err != nil {
				return nil, err
			}
			return nil, &HTTPError{
				StatusCode: resp.StatusCode,
				Reason:     "authentication/authorization failure",
				Body:       string(body),
				Headers:    resp.Header.Clone(),
			}
		}

		// 2) 200 OK, but a header indicates an auth/login problem
		if resp.StatusCode == http.StatusOK && opts.AuthHeaderPolicy != nil {
			val := strings.TrimSpace(resp.Header.Get(opts.AuthHeaderPolicy.HeaderName))
			if val != "" && opts.AuthHeaderPolicy.IsProblem(val) {
				body, _ := io.ReadAll(resp.Body)
				err := resp.Body.Close()
				if err != nil {
					return nil, err
				}
				return nil, &HTTPError{
					StatusCode: resp.StatusCode,
					Reason:     fmt.Sprintf("auth/login issue indicated by %s=%q", opts.AuthHeaderPolicy.HeaderName, val),
					Body:       string(body),
					Headers:    resp.Header.Clone(),
				}
			}
		}

		// 3) 429 Too Many Requests => retry with Retry-After or backoff
		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt >= opts.MaxRetries {
				body, _ := io.ReadAll(resp.Body)
				err := resp.Body.Close()
				if err != nil {
					return nil, err
				}
				return nil, &HTTPError{
					StatusCode: resp.StatusCode,
					Reason:     "rate limit exceeded; retries exhausted",
					Body:       string(body),
					Headers:    resp.Header.Clone(),
				}
			}

			delay := backoffWithJitter(opts.BaseDelay, opts.MaxDelay, attempt)
			if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil && secs >= 0 {
					delay = time.Duration(secs) * time.Second
				}
			}
			err := resp.Body.Close()
			if err != nil {
				return nil, err
			}

			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, context.Canceled
			case <-t.C:
			}
			attempt++
			continue
		}

		// 4) Other non-2xx => return as error (no auto-retry here)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			err := resp.Body.Close()
			if err != nil {
				return nil, err
			}
			return nil, &HTTPError{
				StatusCode: resp.StatusCode,
				Reason:     http.StatusText(resp.StatusCode),
				Body:       string(body),
				Headers:    resp.Header.Clone(),
			}
		}

		defer resp.Body.Close()

		// Read the response body for context
		bodyBytes, _ := io.ReadAll(resp.Body)
		return bodyBytes, nil
	}
}
