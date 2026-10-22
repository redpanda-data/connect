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
	"strings"
)

// Kind is the policy class of an error from this package. Callers switch on
// it instead of chaining predicates; the mapping from Snowflake's codes to a
// Kind lives here so the policy table is in one place.
type Kind int

const (
	// KindOther is anything not classified below: transport errors, unknown
	// HTTP failures, decode errors. Callers treat it as a failed request.
	KindOther Kind = iota
	// KindBackpressure is a retryable saturation signal (429, 503, or one
	// of backpressureErrorCodes). The channel stays usable.
	KindBackpressure
	// KindOpenCollision is the 409 for two opens racing on one channel name.
	KindOpenCollision
	// KindUncommittedData is the 409 for opening a channel that still has
	// appended-but-uncommitted rows.
	KindUncommittedData
	// KindLocalEncoding is a failure before any request was made
	// (ErrLocalEncoding); the channel is untouched.
	KindLocalEncoding
	// KindChannelReopen is a non-SUCCESS channel status that reopening the
	// channel fixes, per Snowflake's error-handling guidance.
	KindChannelReopen
	// KindChannelOperator is a non-SUCCESS channel status an operator must
	// fix first (pipe or table missing or unauthorised, a schema-evolution
	// failure caused by the data). Reopening alone does not help, and
	// retrying the batch repeats the failure.
	KindChannelOperator
)

func (k Kind) String() string {
	switch k {
	case KindBackpressure:
		return "backpressure"
	case KindOpenCollision:
		return "open-collision"
	case KindUncommittedData:
		return "uncommitted-data"
	case KindLocalEncoding:
		return "local-encoding"
	case KindChannelReopen:
		return "channel-reopen"
	case KindChannelOperator:
		return "channel-operator"
	default:
		return "other"
	}
}

// ChannelStatusError is a channel_status_code other than SUCCESS, observed
// on open or while waiting for a commit. Snowflake's error-handling page
// lists which codes a reopen fixes and which need an operator first; see
// channelStatusFamilies.
type ChannelStatusError struct {
	Channel     string
	Code        string
	Message     string
	OnOpen      bool   // seen in the open-channel response rather than while waiting
	OffsetToken string // the token being waited for, when not OnOpen
}

func (e *ChannelStatusError) Error() string {
	if e.OnOpen {
		return fmt.Sprintf("channel %q opened with status %q: %s", e.Channel, e.Code, e.Message)
	}
	return fmt.Sprintf("channel %q status %s while waiting for offset %q: %s", e.Channel, e.Code, e.OffsetToken, e.Message)
}

// channelStatusFamilies maps the status codes Snowflake documents to the
// action they need. Codes not listed, and the ERR_CHANNEL_SCHEMA_EVOLUTION_
// prefix, are handled in classifyStatusCode.
var channelStatusFamilies = map[string]Kind{
	"ERR_CHANNEL_MUST_BE_REOPENED":                    KindChannelReopen,
	"ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP": KindChannelReopen,
	"ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER":           KindChannelReopen,
	"ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER":        KindChannelReopen,
	"ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED":       KindChannelOperator,
	"ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED":         KindChannelOperator,
}

// classifyStatusCode returns the family of a non-SUCCESS channel status
// code. Schema-evolution failures are caused by the data meeting the table
// (an unmappable column, too many new columns, an unsupported Iceberg
// type), so they need an operator. An unlisted code gets the reopen family,
// which is the conservative action: reopen, retry, and surface the code.
func classifyStatusCode(code string) Kind {
	if k, ok := channelStatusFamilies[code]; ok {
		return k
	}
	if strings.HasPrefix(code, "ERR_CHANNEL_SCHEMA_EVOLUTION_") {
		return KindChannelOperator
	}
	return KindChannelReopen
}

// Classify returns err's Kind. It looks through wrapping, so callers can
// classify an error that has been annotated on the way up.
func Classify(err error) Kind {
	if err == nil {
		return KindOther
	}
	if cse, ok := errors.AsType[*ChannelStatusError](err); ok {
		return classifyStatusCode(cse.Code)
	}
	if errors.Is(err, ErrLocalEncoding) {
		return KindLocalEncoding
	}
	switch {
	case IsOpenChannelInProgress(err):
		return KindOpenCollision
	case IsUncommittedDataConflict(err):
		return KindUncommittedData
	case IsBackpressure(err):
		return KindBackpressure
	}
	return KindOther
}

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
	if hse, ok := errors.AsType[*httpStatusError](err); ok {
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
