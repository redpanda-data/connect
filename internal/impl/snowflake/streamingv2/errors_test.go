// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsBackpressureClassifiesStatusCodes pins IsBackpressure's two
// independent signals: the HTTP status code alone (429/503, matching the
// Kafka Connector's BackpressureException) and, separately, an
// backpressureErrorCodes-named error_code in the body even under a status
// that isn't 429/503 on its own.
func TestIsBackpressureClassifiesStatusCodes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		body       string
		want       bool
	}{
		{"429 alone classifies", 429, "", true},
		{"503 alone classifies", 503, "", true},
		{"400 alone does not classify", 400, "", false},
		{"401 alone does not classify", 401, "", false},
		{"404 alone does not classify", 404, "", false},
		{"500 alone does not classify", 500, "", false},
		{"400 with ReceiverSaturated body still classifies", 400, `{"error_code":"ReceiverSaturated"}`, true},
		{"400 with MemoryThresholdExceeded body still classifies", 400, `{"error_code":"MemoryThresholdExceeded"}`, true},
		{"400 with MemoryThresholdExceededInContainer body still classifies", 400, `{"error_code":"MemoryThresholdExceededInContainer"}`, true},
		{"400 with HttpRetryableClientError body still classifies", 400, `{"error_code":"HttpRetryableClientError"}`, true},
		{"400 with an unrelated error_code does not classify", 400, `{"error_code":"SomeOtherError"}`, false},
		{"400 with a non-JSON body does not classify", 400, "not json at all", false},

		// ERR_GENERAL_EXCEPTION_RETRY_REQUEST arrives in two shapes and with
		// two statuses. The live 503 carried a bare JSON string; GS's
		// createInternalServerErrorResponse always returns 500. Classifying on
		// the code rather than the status is what makes both retryable.
		{"500 with a bare-string retry-request body classifies", 500, `"ERR_GENERAL_EXCEPTION_RETRY_REQUEST"`, true},
		{"503 with a bare-string retry-request body classifies", 503, `"ERR_GENERAL_EXCEPTION_RETRY_REQUEST"`, true},
		{"500 with an object-form retry-request body classifies", 500, `{"error_code":"ERR_GENERAL_EXCEPTION_RETRY_REQUEST"}`, true},
		{"500 with an unrelated bare-string body does not classify", 500, `"ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"`, false},
		{"400 with a bare-string retry-request body classifies", 400, `"ERR_GENERAL_EXCEPTION_RETRY_REQUEST"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &httpStatusError{method: "POST", path: "/v2/streaming/x", statusCode: tc.statusCode, body: tc.body}
			if got := IsBackpressure(err); got != tc.want {
				t.Errorf("IsBackpressure(status=%d, body=%q) = %v, want %v", tc.statusCode, tc.body, got, tc.want)
			}
		})
	}
}

// TestIsBackpressureFalseForNonHTTPStatusError confirms a transport-level
// failure that never reached the server (no httpStatusError anywhere in its
// chain) is never treated as backpressure -- there's no status to retry on.
func TestIsBackpressureFalseForNonHTTPStatusError(t *testing.T) {
	if IsBackpressure(errors.New("dial tcp: connection refused")) {
		t.Error("a plain error carrying no HTTP status must not classify as backpressure")
	}
}

// TestIsBackpressureSeesThroughWrapping confirms IsBackpressure unwraps the
// %w chains AppendRows and OpenChannel add around Client.do's error (e.g.
// `append rows to channel %q: %w`), matching statusCodeOf's errors.As use.
func TestIsBackpressureSeesThroughWrapping(t *testing.T) {
	base := &httpStatusError{method: "POST", path: "/v2/streaming/data/x/channels/c/rows", statusCode: 429}
	wrapped := fmt.Errorf("append rows to channel %q: %w", "c", base)
	if !IsBackpressure(wrapped) {
		t.Error("IsBackpressure must see through fmt.Errorf(%w) wrapping")
	}
}

// TestIsOpenChannelInProgressClassifiesExactCode pins IsOpenChannelInProgress
// to the one 409 error_code it must match, and confirms it does NOT fire on a
// 409 with an unrelated code (ERR_PIPE_IN_INVALID_STATE, a real 409 from a
// different endpoint), on a matching code at a different status, or on a
// non-httpStatusError err.
func TestIsOpenChannelInProgressClassifiesExactCode(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		body       string
		want       bool
	}{
		{"409 ERR_OPEN_CHANNEL_IN_PROGRESS classifies", 409, `{"error_code":"ERR_OPEN_CHANNEL_IN_PROGRESS"}`, true},
		{"409 with unrelated code does not classify", 409, `{"error_code":"ERR_PIPE_IN_INVALID_STATE"}`, false},
		{"409 with no body does not classify", 409, "", false},
		{"429 with the matching code does not classify (wrong status)", 429, `{"error_code":"ERR_OPEN_CHANNEL_IN_PROGRESS"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &httpStatusError{method: "PUT", path: "/v2/streaming/x/channels/c", statusCode: tc.statusCode, body: tc.body}
			if got := IsOpenChannelInProgress(err); got != tc.want {
				t.Errorf("IsOpenChannelInProgress(status=%d, body=%q) = %v, want %v", tc.statusCode, tc.body, got, tc.want)
			}
		})
	}
	if IsOpenChannelInProgress(errors.New("dial tcp: connection refused")) {
		t.Error("a plain error carrying no HTTP status must not classify as an open-channel-in-progress conflict")
	}
}

// TestIsOpenChannelInProgressSeesThroughWrapping mirrors
// TestIsBackpressureSeesThroughWrapping for the %w wrapping OpenChannel adds
// around Client.do's error (`open channel %q: %w`).
func TestIsOpenChannelInProgressSeesThroughWrapping(t *testing.T) {
	base := &httpStatusError{method: "PUT", path: "/v2/streaming/x/channels/c", statusCode: 409, body: `{"error_code":"ERR_OPEN_CHANNEL_IN_PROGRESS"}`}
	wrapped := fmt.Errorf("open channel %q: %w", "c", base)
	if !IsOpenChannelInProgress(wrapped) {
		t.Error("IsOpenChannelInProgress must see through fmt.Errorf(%w) wrapping")
	}
}

// TestIsUncommittedDataConflictClassifiesExactCode mirrors
// TestIsOpenChannelInProgressClassifiesExactCode for the sibling 409:
// IsUncommittedDataConflict must match status=409 AND
// error_code=ERR_CHANNEL_HAS_UNCOMMITTED_DATA, and nothing else -- in
// particular NOT the other channel-open 409 (ERR_OPEN_CHANNEL_IN_PROGRESS),
// which is the exact case a lumped-together implementation would get wrong.
func TestIsUncommittedDataConflictClassifiesExactCode(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		body       string
		want       bool
	}{
		{"409 ERR_CHANNEL_HAS_UNCOMMITTED_DATA classifies", 409, `{"error_code":"ERR_CHANNEL_HAS_UNCOMMITTED_DATA"}`, true},
		{"409 ERR_OPEN_CHANNEL_IN_PROGRESS (the sibling conflict) does not classify", 409, `{"error_code":"ERR_OPEN_CHANNEL_IN_PROGRESS"}`, false},
		{"409 with an unrelated code does not classify", 409, `{"error_code":"ERR_PIPE_IN_INVALID_STATE"}`, false},
		{"409 with no body does not classify", 409, "", false},
		{"429 with the matching code does not classify (wrong status)", 429, `{"error_code":"ERR_CHANNEL_HAS_UNCOMMITTED_DATA"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &httpStatusError{method: "PUT", path: "/v2/streaming/x/channels/c", statusCode: tc.statusCode, body: tc.body}
			if got := IsUncommittedDataConflict(err); got != tc.want {
				t.Errorf("IsUncommittedDataConflict(status=%d, body=%q) = %v, want %v", tc.statusCode, tc.body, got, tc.want)
			}
		})
	}
	if IsUncommittedDataConflict(errors.New("dial tcp: connection refused")) {
		t.Error("a plain error carrying no HTTP status must not classify as an uncommitted-data conflict")
	}
}

// TestIsUncommittedDataConflictSeesThroughWrapping mirrors
// TestIsOpenChannelInProgressSeesThroughWrapping for the %w wrapping
// OpenChannel adds around Client.do's error.
func TestIsUncommittedDataConflictSeesThroughWrapping(t *testing.T) {
	base := &httpStatusError{method: "PUT", path: "/v2/streaming/x/channels/c", statusCode: 409, body: `{"error_code":"ERR_CHANNEL_HAS_UNCOMMITTED_DATA"}`}
	wrapped := fmt.Errorf("open channel %q: %w", "c", base)
	if !IsUncommittedDataConflict(wrapped) {
		t.Error("IsUncommittedDataConflict must see through fmt.Errorf(%w) wrapping")
	}
}

// TestChannelOpen409sAreMutuallyExclusive: for each channel-open 409 code
// exactly one of IsOpenChannelInProgress / IsUncommittedDataConflict fires.
// OpenChannel retries only the first.
func TestChannelOpen409sAreMutuallyExclusive(t *testing.T) {
	for _, tc := range []struct {
		name                string
		errorCode           string
		wantInProgress      bool
		wantUncommittedData bool
	}{
		{"ERR_OPEN_CHANNEL_IN_PROGRESS", errOpenChannelInProgress, true, false},
		{"ERR_CHANNEL_HAS_UNCOMMITTED_DATA", errChannelHasUncommittedData, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &httpStatusError{method: "PUT", path: "/v2/streaming/x/channels/c", statusCode: 409, body: fmt.Sprintf(`{"error_code":%q}`, tc.errorCode)}
			gotInProgress := IsOpenChannelInProgress(err)
			gotUncommittedData := IsUncommittedDataConflict(err)
			if gotInProgress != tc.wantInProgress {
				t.Errorf("IsOpenChannelInProgress(%s) = %v, want %v", tc.errorCode, gotInProgress, tc.wantInProgress)
			}
			if gotUncommittedData != tc.wantUncommittedData {
				t.Errorf("IsUncommittedDataConflict(%s) = %v, want %v", tc.errorCode, gotUncommittedData, tc.wantUncommittedData)
			}
			if gotInProgress && gotUncommittedData {
				t.Errorf("%s classified as BOTH IsOpenChannelInProgress and IsUncommittedDataConflict -- these must be mutually exclusive", tc.errorCode)
			}
		})
	}
}

// TestHTTP404NeverClassifiesAsRetryable: a 404, bare or error-enveloped, is
// never classified as backpressure or as either channel-open conflict, so no
// retry path can be reached from it.
func TestHTTP404NeverClassifiesAsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"bare 404, no body", ""},
		{"404 with a well-formed error envelope", `{"error_code":"ERR_SOMETHING","message":"not found"}`},
		{"404 with an empty error envelope", `{"error_code":"","message":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &httpStatusError{method: "GET", path: "/v2/streaming/hostname", statusCode: 404, body: tc.body}
			if IsBackpressure(err) {
				t.Errorf("IsBackpressure(404, body=%q) = true, want false", tc.body)
			}
			if IsOpenChannelInProgress(err) {
				t.Errorf("IsOpenChannelInProgress(404, body=%q) = true, want false", tc.body)
			}
			if IsUncommittedDataConflict(err) {
				t.Errorf("IsUncommittedDataConflict(404, body=%q) = true, want false", tc.body)
			}
			if code, ok := statusCodeOf(err); !ok || code != 404 {
				t.Fatalf("statusCodeOf = (%d, %v), want (404, true)", code, ok)
			}
		})
	}
}
