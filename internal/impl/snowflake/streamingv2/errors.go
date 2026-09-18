// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"encoding/json"
	"errors"
	"fmt"
)

// httpStatusError carries a non-2xx HTTP response's status and body so
// callers can classify the failure programmatically (see IsBackpressure)
// instead of pattern-matching Error()'s text. Its Error() format is kept
// byte-identical to the plain fmt.Errorf this type replaced in Client.do, so
// existing tests asserting on that text (e.g. TestNonSuccessSurfacesStatusAndBody)
// continue to pass unchanged.
type httpStatusError struct {
	method     string
	path       string
	statusCode int
	body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.method, e.path, e.statusCode, e.body)
}

// backpressureErrorCodes mirrors the Kafka Connector's BackpressureException:
// these are the only SFException error-code names it treats as a retryable
// backpressure signal rather than a hard failure.
var backpressureErrorCodes = map[string]bool{
	"ReceiverSaturated":                  true,
	"MemoryThresholdExceeded":            true,
	"MemoryThresholdExceededInContainer": true,
	"HttpRetryableClientError":           true,

	// Not one of the Kafka Connector's codes: this is Snowflake's own, and the
	// server names it after the action it wants ("retry request"). GS emits it
	// for transient internal conditions -- a pipe not yet locally owned or
	// started on the node handling the request, a pipe manager not yet
	// running, a SQL exception resolving the pipe master key -- all of which
	// clear on their own.
	//
	// Keyed by code rather than by status deliberately, because the status is
	// not a reliable handle on it. GS ships it through
	// RowsetResourceUtils.createInternalServerErrorResponse, which always
	// returns 500, while the instance measured on a live account arrived as
	// 503. A status-only rule catches whichever variant you happened to
	// observe and misses the other; classifying on the code catches both.
	"ERR_GENERAL_EXCEPTION_RETRY_REQUEST": true,
}

// errorCodeBody is the minimal shape needed to read a response body's
// error_code field. A body that isn't JSON, or has no such field, yields an
// empty code -- this is a best-effort signal layered on top of the
// status-code check in IsBackpressure, not a required one, so a decode
// failure here must not itself become an error.
type errorCodeBody struct {
	ErrorCode string `json:"error_code"`
}

func errorCodeFromBody(body string) string {
	var b errorCodeBody
	if err := json.Unmarshal([]byte(body), &b); err == nil && b.ErrorCode != "" {
		return b.ErrorCode
	}
	// The API also returns a bare JSON string for some errors instead of an
	// object. Measured on a live account, the status endpoint's 503 body was
	// exactly `"ERR_GENERAL_EXCEPTION_RETRY_REQUEST"` -- no enclosing object,
	// no error_code field -- while a 404 from the same pipe path came back as
	// `{"error_code": "ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED", ...}`.
	// Reading only the object form silently yields no code for the string
	// form, which would leave any code-keyed classification unreachable for
	// precisely the errors that arrive that way.
	//
	// Decoded rather than substring-matched deliberately: a body that merely
	// mentions a code inside prose is not the same as a body whose entire
	// content is that code, and loose matching on names is how a guard ends up
	// firing on things that just happen to contain the string.
	var s string
	if err := json.Unmarshal([]byte(body), &s); err == nil {
		return s
	}
	return ""
}

// statusCodeOf extracts the HTTP status carried by err, if any. It unwraps
// through the error chain via errors.As, so it sees through the %w wrapping
// AppendRows and OpenChannel add around Client.do's error.
func statusCodeOf(err error) (int, bool) {
	var hse *httpStatusError
	if errors.As(err, &hse) {
		return hse.statusCode, true
	}
	return 0, false
}

// errOpenChannelInProgress is the error_code the SSv2 API answers with when
// an HTTP 409 on open-channel means "another open for this same channel name
// is already in flight" -- see IsOpenChannelInProgress. It is scoped
// narrowly to that one code: a 409 with any other error_code is a different,
// non-retryable failure (ERR_PIPE_IN_INVALID_STATE, a real 409 code from a
// different endpoint, is an example of a 409 this must NOT match).
const errOpenChannelInProgress = "ERR_OPEN_CHANNEL_IN_PROGRESS"

// IsOpenChannelInProgress reports whether err is the specific 409 the SSv2
// API returns for a channel-open collision: two opens racing for the same
// channel name, which is the ordinary case under concurrent load (one
// channel per Kafka partition, with concurrent opens expected) rather than a
// hard failure. Both conditions are required -- status 409 AND
// error_code=ERR_OPEN_CHANNEL_IN_PROGRESS -- so a 409 carrying a different
// code is deliberately NOT treated as retryable here.
func IsOpenChannelInProgress(err error) bool {
	var hse *httpStatusError
	if !errors.As(err, &hse) {
		return false
	}
	return hse.statusCode == 409 && errorCodeFromBody(hse.body) == errOpenChannelInProgress
}

// errChannelHasUncommittedData is the other 409 error_code channel open can
// answer with: the channel already has data appended that has not yet
// committed, so opening it again (e.g. to reset a stuck continuation token)
// would race the pending commit. See IsUncommittedDataConflict.
const errChannelHasUncommittedData = "ERR_CHANNEL_HAS_UNCOMMITTED_DATA"

// IsUncommittedDataConflict reports whether err is the 409 channel-open
// conflict distinct from IsOpenChannelInProgress: error_code=
// ERR_CHANNEL_HAS_UNCOMMITTED_DATA rather than ERR_OPEN_CHANNEL_IN_PROGRESS.
//
// OpenChannel deliberately does NOT retry or force past this one -- see its
// doc comment in channel.go. This function exists only so a caller can
// classify and surface the conflict (e.g. in logs/metrics) with its specific
// error_code, not so anything here can react to it. Whether to wait for the
// pending commit (and for how long), or to force a reopen that discards the
// uncommitted rows (a fail_on_uncommitted_rows-style opt-in, a pattern
// already adopted elsewhere, by Openflow, on its own
// openChannel/dropChannel), is a data-safety decision left to an operator.
// Neither is decided or implemented here.
func IsUncommittedDataConflict(err error) bool {
	var hse *httpStatusError
	if !errors.As(err, &hse) {
		return false
	}
	return hse.statusCode == 409 && errorCodeFromBody(hse.body) == errChannelHasUncommittedData
}

// IsBackpressure reports whether err represents a retryable backpressure
// failure rather than a hard error, mirroring the Kafka Connector's
// BackpressureException: HTTP 429 (Too Many Requests) or 503 (Service
// Unavailable) alone is sufficient, and a response body naming one of the
// four backpressureErrorCodes is an independent, additional signal for a
// non-2xx response whose status is something else entirely -- the
// ERR_GENERAL_EXCEPTION_RETRY_REQUEST case above is exactly this, observed
// as both a 500 and a 503 for the same underlying condition, which a
// status-only check would only catch on one of the two.
//
// This can only ever fire against a non-2xx response: do() turns every 2xx
// into a plain success with no error at all, regardless of what the body
// says, so a body-only backpressure signal riding on a 2xx status (if
// Snowflake's API ever does that on some endpoint) would not be visible to
// this function today -- it has no error to inspect. Nothing here has
// observed that happening; noted as a known gap, not a handled case.
//
// A non-httpStatusError err (e.g. a transport-level failure that never
// reached the server) is never backpressure.
func IsBackpressure(err error) bool {
	var hse *httpStatusError
	if !errors.As(err, &hse) {
		return false
	}
	if hse.statusCode == 429 || hse.statusCode == 503 {
		return true
	}
	return backpressureErrorCodes[errorCodeFromBody(hse.body)]
}
