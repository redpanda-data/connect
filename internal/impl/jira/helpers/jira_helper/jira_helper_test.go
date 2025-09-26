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

package jira_helper

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// stubRC is a simple ReadCloser that records whether Close was called.
type stubRC struct {
	r      io.Reader
	closed bool
}

func (s *stubRC) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *stubRC) Close() error               { s.closed = true; return nil }

func newResp(status int, body string, hdr http.Header) *http.Response {
	if hdr == nil {
		hdr = make(http.Header)
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     hdr,
	}
}

func TestCheckJiraAuth_NilResponse(t *testing.T) {
	_, err := CheckJiraAuth(nil)
	if err == nil {
		t.Fatalf("expected error for nil response; got nil")
	}
}

func TestCheckJiraAuth_401_Unauthorized(t *testing.T) {
	const body = "bad credentials"
	resp := newResp(http.StatusUnauthorized, body, nil)

	bodyBytes, err := CheckJiraAuth(resp)
	bodyString := string(bodyBytes)
	if err == nil {
		t.Fatalf("expected JiraError for 401; got nil")
	}
	var je *JiraError
	if !errors.As(err, &je) {
		t.Fatalf("expected *JiraError; got %T", err)
	}
	if je.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode=%d; want 401", je.StatusCode)
	}
	if !strings.Contains(je.Reason, "Unauthorized") {
		t.Fatalf("Reason=%q; want contains 'Unauthorized'", je.Reason)
	}
	if bodyString != body {
		t.Fatalf("Body=%q; want %q", bodyString, body)
	}
}

func TestCheckJiraAuth_403_Forbidden(t *testing.T) {
	const body = "forbidden"
	resp := newResp(http.StatusForbidden, body, nil)

	bodyBytes, err := CheckJiraAuth(resp)
	bodyString := string(bodyBytes)
	if err == nil {
		t.Fatalf("expected JiraError for 403; got nil")
	}
	var je *JiraError
	if !errors.As(err, &je) {
		t.Fatalf("expected *JiraError; got %T", err)
	}
	if je.StatusCode != http.StatusForbidden {
		t.Fatalf("StatusCode=%d; want 403", je.StatusCode)
	}
	if !strings.Contains(strings.ToLower(je.Reason), "forbidden") {
		t.Fatalf("Reason=%q; want contains 'Forbidden'", je.Reason)
	}
	if bodyString != body {
		t.Fatalf("Body=%q; want %q", bodyString, body)
	}
}

func TestCheckJiraAuth_429_TooManyRequests_WithRetryAfter(t *testing.T) {
	const body = "slow down"
	h := make(http.Header)
	h.Set("Retry-After", "7")
	resp := newResp(http.StatusTooManyRequests, body, h)

	bodyBytes, err := CheckJiraAuth(resp)
	bodyString := string(bodyBytes)
	if err == nil {
		t.Fatalf("expected JiraError for 429; got nil")
	}
	var je *JiraError
	if !errors.As(err, &je) {
		t.Fatalf("expected *JiraError; got %T", err)
	}
	if je.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("StatusCode=%d; want 429", je.StatusCode)
	}
	if bodyString != body {
		t.Fatalf("Body=%q; want %q", bodyString, body)
	}
	if je.Headers.Get("Retry-After") != "7" {
		t.Fatalf("Retry-After=%q; want %q", je.Headers.Get("Retry-After"), "7")
	}
}

func TestCheckJiraAuth_200_WithAuthHeaderProblem(t *testing.T) {
	const body = "looks fine but actually auth issue"
	h := make(http.Header)
	h.Set("X-Seraph-LoginReason", "AUTHENTICATION_DENIED")
	resp := newResp(http.StatusOK, body, h)

	_, err := CheckJiraAuth(resp)
	if err == nil {
		t.Fatalf("expected JiraError for 200 with X-Seraph-LoginReason; got nil")
	}
	var je *JiraError
	if !errors.As(err, &je) {
		t.Fatalf("expected *JiraError; got %T", err)
	}
	if je.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode=%d; want 200", je.StatusCode)
	}
	if !strings.Contains(je.Reason, "X-Seraph-LoginReason") {
		t.Fatalf("Reason=%q; want mention of X-Seraph-LoginReason", je.Reason)
	}
	if je.Headers.Get("X-Seraph-LoginReason") != "AUTHENTICATION_DENIED" {
		t.Fatalf("header value mismatch; got %q", je.Headers.Get("X-Seraph-LoginReason"))
	}
}

func TestCheckJiraAuth_200_OK_NoAuthHeader(t *testing.T) {
	const body = `{"ok":true}`
	resp := newResp(http.StatusOK, body, nil)

	_, err := CheckJiraAuth(resp)
	if err != nil {
		t.Fatalf("expected nil for clean 200; got %v", err)
	}
}

func TestCheckJiraAuth_200_OK_WithBenignAuthHeaderValues(t *testing.T) {
	// Values specifically allowed by the implementation: "OK", "AUTHENTICATED_TRUE"
	tests := []string{"OK", "AUTHENTICATED_TRUE"}
	for _, v := range tests {
		h := make(http.Header)
		h.Set("X-Seraph-LoginReason", v)
		resp := newResp(http.StatusOK, "ok", h)
		if _, err := CheckJiraAuth(resp); err != nil {
			t.Fatalf("expected nil for 200 with %s; got %v", v, err)
		}
	}
}

func TestCheckJiraAuth_500_GenericServerError(t *testing.T) {
	const body = "boom"
	resp := newResp(http.StatusInternalServerError, body, nil)

	bodyBytes, err := CheckJiraAuth(resp)
	bodyString := string(bodyBytes)
	if err == nil {
		t.Fatalf("expected JiraError for 500; got nil")
	}
	var je *JiraError
	if !errors.As(err, &je) {
		t.Fatalf("expected *JiraError; got %T", err)
	}
	if je.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode=%d; want 500", je.StatusCode)
	}
	if je.Reason != http.StatusText(http.StatusInternalServerError) {
		t.Fatalf("Reason=%q; want %q", je.Reason, http.StatusText(http.StatusInternalServerError))
	}
	if bodyString != body {
		t.Fatalf("Body=%q; want %q", bodyString, body)
	}
}

func TestJiraError_ErrorString(t *testing.T) {
	je := &JiraError{
		StatusCode: 401,
		Reason:     "Unauthorized – likely invalid API token or username",
		Headers:    http.Header{"X-Test": []string{"v"}},
	}
	got := je.Error()
	if !strings.Contains(got, "Jira API error: 401") || !strings.Contains(got, "Unauthorized") {
		t.Fatalf("Error() string unexpected: %q", got)
	}
}

func TestCheckJiraAuth_ClosesBody_OnError(t *testing.T) {
	rc := &stubRC{r: strings.NewReader("content")}
	resp := &http.Response{
		StatusCode: http.StatusUnauthorized,
		Body:       rc,
		Header:     make(http.Header),
	}
	_, _ = CheckJiraAuth(resp)
	if !rc.closed {
		t.Fatalf("expected response body to be closed on error")
	}
}
