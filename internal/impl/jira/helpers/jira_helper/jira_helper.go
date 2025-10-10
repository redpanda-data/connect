// Copyright 2024 Redpanda Data, Inc.
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

/*
Package jira_helper provides helpers for making HTTP requests
with Jira-specific authentication and rate-limiting handling.
It wraps HTTP responses to detect common error cases such as
401/403 Unauthorized, 429 Too Many Requests, and Jira’s
X-Seraph-LoginReason header, which may indicate an authentication
problem even on a 200 OK response.

Package jira_helper usage documentation

Overview
  - Provides helpers for interpreting Jira HTTP responses with a focus on
    authentication and rate-limiting signals.
  - Central entry point: CheckJiraAuth(resp) which examines an http.Response and
    returns a *JiraError for common Jira conditions:
  - 401 Unauthorized: Likely invalid credentials (email/API token).
  - 403 Forbidden: Authenticated but insufficient permissions.
  - 429 Too Many Requests: Jira is throttling; check Retry-After header.
  - 200 OK with X-Seraph-LoginReason indicating an auth issue.
  - On success (no issues detected), CheckJiraAuth returns nil.
  - On failure, JiraError includes StatusCode, Reason, Body, and Headers.

When to use
  - Immediately after getting an *http.Response from Jira (e.g., client.Do(req)),
    before processing the body. This gives a consistent, structured way to react
    to auth and rate-limit conditions.

Quick Start

 1. Basic request and auth check

    ctx := context.Background()
    req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https:your-domain.atlassian.net/rest/api/3/myself", nil)
    req.SetBasicAuth("<jira-email>", "<jira-api-token>")
    req.Header.Set("Accept", "application/json")
    req.Header.Set("User-Agent", "YourApp/1.0")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
    transport or network error
    log.Fatalf("request failed: %v", err)
    }
    defer resp.Body.Close()

    if jerr := jira_helper.CheckJiraAuth(resp); jerr != nil {
    // This includes 401, 403, 429, and 200 + X-Seraph-LoginReason problems.
    // You can inspect the error for details.
    if je, ok := jerr.(*jira_helper.JiraError); ok {
    log.Printf("jira error: status=%d reason=%s", je.StatusCode, je.Reason)
    log.Printf("headers: %v", je.Headers)
    //Optionally log or parse a truncated version of je.Body.
    }
    return
    }

    // Safe to read/parse the response body here.
    data, _ := io.ReadAll(resp.Body)
    log.Printf("success: %s", string(data))

 2. Handling rate limiting (429) with Retry-After

    // If CheckJiraAuth returns a JiraError with StatusCode 429, look for Retry-After.
    jerr := jira_helper.CheckJiraAuth(resp)
    if jerr != nil {
    if je, ok := jerr.(*jira_helper.JiraError); ok && je.StatusCode == http.StatusTooManyRequests {
    retryAfter := je.Headers.Get("Retry-After") // integer seconds expected
    // Convert to a duration and sleep before retrying
    if secs, convErr := strconv.Atoi(strings.TrimSpace(retryAfter)); convErr == nil && secs >= 0 {
    time.Sleep(time.Duration(secs) * time.Second)
    //retry the request here
    } else {
    // Fallback: use a default backoff (e.g., exponential with jitter) before retry
    }
    }
    }

 3. Interpreting X-Seraph-LoginReason on 200 OK

    // Jira sometimes returns 200 OK while signaling auth issues via X-Seraph-LoginReason.
    // CheckJiraAuth covers this case by returning a JiraError if the header indicates a problem.
    if jerr := jira_helper.CheckJiraAuth(resp); jerr != nil {
    if je, ok := jerr.(*jira_helper.JiraError); ok && je.StatusCode == http.StatusOK {
    //Example reasons might include "AUTHENTICATION_DENIED" or "AUTHENTICATED_FAILED".
    log.Printf("auth header issue: %s", je.Reason)
    // Prompt for re-authentication, rotate tokens, etc.
    }
    }

 4. Centralized error handling

    jerr := jira_helper.CheckJiraAuth(resp)
    if jerr != nil {
    // Use errors.As to extract *JiraError
    var je *jira_helper.JiraError
    if errors.As(jerr, &je) {
    switch je.StatusCode {
    case http.StatusUnauthorized:
    // refresh token, prompt re-login, etc.
    case http.StatusForbidden:
    // insufficient permissions: inform user or adjust scopes
    case http.StatusTooManyRequests:
    // back off and retry later
    default:
    // 200 with X-Seraph-LoginReason or other 4xx/5xx
    }
    }
    return
    }
    // proceed to parse resp.Body

 5. Example helper wrapping a Jira call

    func callJira(ctx context.Context, client *http.Client, req *http.Request) ([]byte, *jira_helper.JiraError) {
    resp, err := client.Do(req.WithContext(ctx))
    if err != nil {
    return nil, &jira_helper.JiraError{
    StatusCode: 0,
    Reason: fmt.Sprintf("transport error: %v", err),
    }
    }
    defer resp.Body.Close()

    if jerr := jira_helper.CheckJiraAuth(resp); jerr != nil {
    if je, ok := jerr.(*jira_helper.JiraError); ok {
    return nil, je
    }
    // Wrap non-JiraError just in case (shouldn't happen).
    return nil, &jira_helper.JiraError{StatusCode: resp.StatusCode, Reason: jerr.Error()}
    }

    data, readErr := io.ReadAll(resp.Body)
    if readErr != nil {
    return nil, &jira_helper.JiraError{StatusCode: resp.StatusCode, Reason: fmt.Sprintf("read error: %v", readErr)}
    }
    return data, nil
    }

Inspecting JiraError
- On error, CheckJiraAuth returns *JiraError containing:
  - StatusCode: HTTP status (or 200 in header-signaled cases).
  - Reason: High-level explanation (or http.StatusText for generic 4xx/5xx).
  - Body: Response body string (caller may truncate before logging).
  - Headers: Cloned response headers for further inspection.

- Example:

	if jerr := jira_helper.CheckJiraAuth(resp); jerr != nil {
	    if je, ok := jerr.(*jira_helper.JiraError); ok {
	        fmt.Printf("status=%d reason=%s\n", je.StatusCode, je.Reason)
	        fmt.Printf("x-seraph=%q\n", je.Headers.Get("X-Seraph-LoginReason"))
	    }
	}

Best Practices
- Always set a User-Agent to identify your application.
- Use context timeouts or deadlines to avoid hanging requests.
- Be mindful when logging bodies; they may contain sensitive or large content.
- Respect Retry-After when present; otherwise choose a conservative backoff.
- Prefer idempotent methods for automatic retries; confirm business safety for POST/PUT.
- Consider centralizing Jira error handling via CheckJiraAuth in your HTTP layer.

Dependencies
- Standard library only: net/http, io, fmt.
*/
package jira_helper

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
// even on 200 OK responses (e.g., "X-Seraph-LoginReason").
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
