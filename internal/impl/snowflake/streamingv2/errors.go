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

// httpStatusError carries a non-2xx response's status and body so callers can
// classify the failure (see IsBackpressure) without parsing Error() text.
type httpStatusError struct {
	method     string
	path       string
	statusCode int
	body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.method, e.path, e.statusCode, e.body)
}

// backpressureErrorCodes are the error codes treated as retryable
// backpressure: the four the Kafka Connector's BackpressureException uses,
// plus Snowflake's own ERR_GENERAL_EXCEPTION_RETRY_REQUEST, which arrives as
// either a 500 or a 503 and so must be matched by code rather than status.
var backpressureErrorCodes = map[string]bool{
	"ReceiverSaturated":                   true,
	"MemoryThresholdExceeded":             true,
	"MemoryThresholdExceededInContainer":  true,
	"HttpRetryableClientError":            true,
	"ERR_GENERAL_EXCEPTION_RETRY_REQUEST": true,
}

type errorCodeBody struct {
	ErrorCode string `json:"error_code"`
}

// errorCodeFromBody reads an error code from a response body, which the API
// sends either as {"error_code": ...} or as a bare JSON string. Best-effort:
// anything else yields "".
func errorCodeFromBody(body string) string {
	var b errorCodeBody
	if err := json.Unmarshal([]byte(body), &b); err == nil && b.ErrorCode != "" {
		return b.ErrorCode
	}
	var s string
	if err := json.Unmarshal([]byte(body), &s); err == nil {
		return s
	}
	return ""
}

// statusCodeOf extracts the HTTP status carried anywhere in err's chain.
func statusCodeOf(err error) (int, bool) {
	var hse *httpStatusError
	if errors.As(err, &hse) {
		return hse.statusCode, true
	}
	return 0, false
}

const errOpenChannelInProgress = "ERR_OPEN_CHANNEL_IN_PROGRESS"

// IsOpenChannelInProgress reports whether err is the 409 for two opens racing
// on the same channel name -- ordinary under concurrent load and retryable.
// Both the status and the error code are required; other 409 codes are not.
func IsOpenChannelInProgress(err error) bool {
	var hse *httpStatusError
	if !errors.As(err, &hse) {
		return false
	}
	return hse.statusCode == 409 && errorCodeFromBody(hse.body) == errOpenChannelInProgress
}

const errChannelHasUncommittedData = "ERR_CHANNEL_HAS_UNCOMMITTED_DATA"

// IsUncommittedDataConflict reports whether err is the 409 for opening a
// channel that still has appended-but-uncommitted data. OpenChannel does not
// retry or force past it (that would discard rows); this lets callers name
// the condition when logging.
func IsUncommittedDataConflict(err error) bool {
	var hse *httpStatusError
	if !errors.As(err, &hse) {
		return false
	}
	return hse.statusCode == 409 && errorCodeFromBody(hse.body) == errChannelHasUncommittedData
}

// IsBackpressure reports whether err is a retryable backpressure failure: an
// HTTP 429 or 503, or any non-2xx whose body names one of
// backpressureErrorCodes. Transport errors that never reached the server are
// not backpressure.
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
