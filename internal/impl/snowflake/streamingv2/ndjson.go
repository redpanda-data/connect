// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package streamingv2 is a client for the Snowpipe Streaming v2 REST API.
package streamingv2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// MaxRequestBytes is the documented per-request limit for the rows endpoint.
// The uncompressed body is checked against it: the server cares about what it
// receives after decompression.
const MaxRequestBytes = 4 << 20

// ErrLocalEncoding wraps every EncodeRows failure. Nothing has reached the
// server when it is returned, so the channel is still healthy and a caller
// should not reopen it.
var ErrLocalEncoding = errors.New("rows cannot be encoded")

// HasLineBreak reports whether b contains a line break, which NDJSON cannot
// carry inside a row.
func HasLineBreak(b []byte) bool {
	return bytes.ContainsAny(b, "\r\n")
}

// SplitRow turns one message body into NDJSON rows. A body without a line
// break is returned as it is, parsed or not: Snowflake reports rows it
// cannot read, and callers rely on that. A body with a line break inside it
// (pretty-printed JSON, or several newline-separated documents) is parsed
// as a sequence of JSON values and each is returned compacted as its own
// row, so a document is never split across rows. An empty body returns no
// rows. The error for a body that does not parse wraps ErrLocalEncoding.
func SplitRow(body []byte) ([][]byte, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, nil
	}
	if !HasLineBreak(body) {
		return [][]byte{body}, nil
	}
	// One document is the common case: compact it in a single pass. The
	// decoder loop below handles several documents in one body.
	compacted := bytes.NewBuffer(make([]byte, 0, len(body)))
	if err := json.Compact(compacted, body); err == nil {
		return [][]byte{compacted.Bytes()}, nil
	}
	var rows [][]byte
	dec := json.NewDecoder(bytes.NewReader(body))
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("%w: body spans several lines and is not valid JSON: %w", ErrLocalEncoding, err)
		}
		compacted := bytes.NewBuffer(make([]byte, 0, len(raw)))
		if err := json.Compact(compacted, raw); err != nil {
			return nil, fmt.Errorf("%w: body spans several lines and is not valid JSON: %w", ErrLocalEncoding, err)
		}
		rows = append(rows, compacted.Bytes())
	}
	return rows, nil
}

// EncodeRows renders rows as newline-delimited JSON. Each row is written
// verbatim apart from surrounding whitespace; empty rows are skipped. A row
// with a line break inside it would be read by the server as several rows,
// so it is an error: callers compact such rows first (json.Compact).
//
// It returns the body to send, the uncompressed byte count (needed for the
// x-snowflake-uncompressed-content-length header), and an error if the batch is
// over MaxRequestBytes. Oversize batches are rejected, never truncated.
func EncodeRows(rows [][]byte, compress bool) ([]byte, int, error) {
	var plain bytes.Buffer
	for i, r := range rows {
		t := bytes.TrimSpace(r)
		if len(t) == 0 {
			continue
		}
		if HasLineBreak(t) {
			return nil, 0, fmt.Errorf("%w: row %d contains a line break and would be sent as several NDJSON rows; compact it first", ErrLocalEncoding, i+1)
		}
		plain.Write(t)
		plain.WriteByte('\n')
	}
	uncompressed := plain.Len()
	if uncompressed > MaxRequestBytes {
		return nil, 0, fmt.Errorf("%w: batch is %d bytes uncompressed, over the %d byte limit; reduce the batching policy", ErrLocalEncoding, uncompressed, MaxRequestBytes)
	}
	if !compress {
		return plain.Bytes(), uncompressed, nil
	}
	var out bytes.Buffer
	enc, err := zstd.NewWriter(&out)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: zstd writer: %w", ErrLocalEncoding, err)
	}
	if _, err := enc.Write(plain.Bytes()); err != nil {
		enc.Close()
		return nil, 0, fmt.Errorf("%w: zstd write: %w", ErrLocalEncoding, err)
	}
	if err := enc.Close(); err != nil {
		return nil, 0, fmt.Errorf("%w: zstd close: %w", ErrLocalEncoding, err)
	}
	return out.Bytes(), uncompressed, nil
}
