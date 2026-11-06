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
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// zstdEncoder is shared by every EncodeRows call: EncodeAll is safe for
// concurrent use and takes one of the encoder's internal states per call,
// so building an encoder per request would allocate that state each time
// while a single shared one still compresses in-flight batches in
// parallel. The state count is capped: each one keeps buffers sized to the
// largest input it saw, and more than a handful of batches are rarely
// being compressed at once.
var (
	zstdEncoder     *zstd.Encoder
	zstdEncoderOnce sync.Once
	zstdEncoderErr  error
)

func sharedZstdEncoder() (*zstd.Encoder, error) {
	zstdEncoderOnce.Do(func() {
		zstdEncoder, zstdEncoderErr = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(min(runtime.GOMAXPROCS(0), 8)))
	})
	return zstdEncoder, zstdEncoderErr
}

// MaxRequestBytes is the documented per-request limit for the rows endpoint:
// "a 4 MB payload limit (the payload size sent over the network, after
// compression if used)". Snowflake writes MB, not MiB, so the decimal value
// is used; the 4.6% left on the table is cheaper than a body in the gap
// being rejected server-side. The body that would go on the wire is checked
// against it, so with compression on a batch may be far larger than this
// uncompressed.
const MaxRequestBytes = 4_000_000

// ErrPayloadTooLarge is returned, wrapping ErrLocalEncoding, when the body
// to send is over MaxRequestBytes. Callers that can split the batch should
// do so; nothing has been sent.
var ErrPayloadTooLarge = fmt.Errorf("%w: payload over the %d byte request limit", ErrLocalEncoding, MaxRequestBytes)

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
// x-snowflake-uncompressed-content-length header), and ErrPayloadTooLarge if
// the body to send is over MaxRequestBytes. Oversize batches are rejected,
// never truncated.
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
	if !compress {
		if uncompressed > MaxRequestBytes {
			return nil, 0, fmt.Errorf("%w: batch is %d bytes uncompressed", ErrPayloadTooLarge, uncompressed)
		}
		return plain.Bytes(), uncompressed, nil
	}
	enc, err := sharedZstdEncoder()
	if err != nil {
		return nil, 0, fmt.Errorf("%w: zstd encoder: %w", ErrLocalEncoding, err)
	}
	out := enc.EncodeAll(plain.Bytes(), make([]byte, 0, uncompressed/4+64))
	if len(out) > MaxRequestBytes {
		return nil, 0, fmt.Errorf("%w: batch is %d bytes after zstd compression (%d uncompressed)", ErrPayloadTooLarge, len(out), uncompressed)
	}
	return out, uncompressed, nil
}
