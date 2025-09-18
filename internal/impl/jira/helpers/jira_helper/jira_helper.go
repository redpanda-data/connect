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
	"errors"
	"fmt"
	"io"
	"net/http"
)

/*
JiraError represents an error returned by the Jira API.
It wraps the HTTP status code, a high-level reason,
the response body (truncated by caller if necessary),
and any relevant response headers.
*/
type JiraError struct {
	StatusCode int
	Reason     string
	Headers    http.Header
}

/*
Error implements the error interface for JiraError,
returning a human-readable error string.
*/
func (e *JiraError) Error() string {
	return fmt.Sprintf("Jira API error: %d %s", e.StatusCode, e.Reason)
}

/*
CheckJiraAuth inspects an HTTP response from Jira and
returns a *JiraError if an authentication or rate-limiting
problem is detected.

The following cases are handled:
  - 401 Unauthorized: Invalid credentials (e.g., bad API token).
  - 403 Forbidden: Valid credentials but insufficient permissions.
  - 429 Too Many Requests: Jira API throttling; caller should retry
    after the delay indicated in the Retry-After header.
  - 200 OK with X-Seraph-LoginReason header: Jira sometimes returns
    200 but indicates authentication issues in this header.

For all other 4xx/5xx statuses, a JiraError is returned with
the status text as the reason.

On success (no detected problem), CheckJiraAuth returns nil
and the caller should proceed to read from the response body.
*/
func CheckJiraAuth(resp *http.Response) ([]byte, error) {
	if resp == nil {
		return nil, errors.New("no response received from Jira API")
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)

	// Read the response body for context
	bodyBytes, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusUnauthorized: // 401
		return bodyBytes, &JiraError{
			StatusCode: resp.StatusCode,
			Reason:     "Unauthorized – likely invalid API token or username",
			Headers:    resp.Header.Clone(),
		}
	case http.StatusForbidden: // 403
		return bodyBytes, &JiraError{
			StatusCode: resp.StatusCode,
			Reason:     "Forbidden – token valid but user lacks required permissions",
			Headers:    resp.Header.Clone(),
		}
	case http.StatusTooManyRequests: // 429
		return bodyBytes, &JiraError{
			StatusCode: resp.StatusCode,
			Reason:     "Rate limit exceeded – Jira API throttling in effect",
			Headers:    resp.Header.Clone(),
		}
	default:
		// Jira sometimes responds 200 but sets X-Seraph-LoginReason
		if resp.StatusCode == http.StatusOK {
			if reason := resp.Header.Get("X-Seraph-LoginReason"); reason != "" && reason != "OK" && reason != "AUTHENTICATED_TRUE" {
				return bodyBytes, &JiraError{
					StatusCode: resp.StatusCode,
					Reason:     fmt.Sprintf("Authentication issue indicated by X-Seraph-LoginReason=%q", reason),
					Headers:    resp.Header.Clone(),
				}
			}
		}
		if resp.StatusCode >= 400 {
			return bodyBytes, &JiraError{
				StatusCode: resp.StatusCode,
				Reason:     http.StatusText(resp.StatusCode),
				Headers:    resp.Header.Clone(),
			}
		}
	}

	// If no error, return only body bytes
	return bodyBytes, nil
}
