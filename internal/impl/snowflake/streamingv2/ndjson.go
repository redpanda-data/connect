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
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// MaxRequestBytes is the documented per-request limit for the rows endpoint.
// The uncompressed body is checked against it: the server cares about what it
// receives after decompression.
const MaxRequestBytes = 4 << 20

// EncodeRows renders rows as newline-delimited JSON. Each row is written
// verbatim apart from surrounding whitespace; empty rows are skipped.
//
// It returns the body to send, the uncompressed byte count (needed for the
// x-snowflake-uncompressed-content-length header), and an error if the batch is
// over MaxRequestBytes. Oversize batches are rejected, never truncated.
func EncodeRows(rows [][]byte, compress bool) ([]byte, int, error) {
	var plain bytes.Buffer
	for _, r := range rows {
		t := bytes.TrimSpace(r)
		if len(t) == 0 {
			continue
		}
		plain.Write(t)
		plain.WriteByte('\n')
	}
	uncompressed := plain.Len()
	if uncompressed > MaxRequestBytes {
		return nil, 0, fmt.Errorf("batch is %d bytes uncompressed, over the %d byte limit; reduce the batching policy", uncompressed, MaxRequestBytes)
	}
	if !compress {
		return plain.Bytes(), uncompressed, nil
	}
	var out bytes.Buffer
	enc, err := zstd.NewWriter(&out)
	if err != nil {
		return nil, 0, fmt.Errorf("zstd writer: %w", err)
	}
	if _, err := enc.Write(plain.Bytes()); err != nil {
		enc.Close()
		return nil, 0, fmt.Errorf("zstd write: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, 0, fmt.Errorf("zstd close: %w", err)
	}
	return out.Bytes(), uncompressed, nil
}
