// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// TestBufferingSinkNewFieldDedup verifies the case-aware dedup behaviour of
// the buffering sink. Without folding, two messages reporting new fields that
// differ only in case (e.g. "FOO" and "foo") would each yield a separate
// UnknownFieldError; the router would then ask iceberg-go to add both, and
// case-insensitive UpdateSchema would reject the second. Folding the dedup
// key collapses them to a single schema-evolution attempt.
func TestBufferingSinkNewFieldDedup(t *testing.T) {
	t.Run("case-insensitive dedup folds case-only duplicates", func(t *testing.T) {
		sink := newBufferingSink(nil, 0, false)
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		errs := sink.newFieldErrors()
		require.Len(t, errs, 1)
		// Whichever arrived first wins; we only assert one survived.
		assert.Contains(t, []string{"FOO", "foo"}, errs[0].FieldName())
	})

	t.Run("case-sensitive dedup keeps both", func(t *testing.T) {
		sink := newBufferingSink(nil, 0, true)
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		errs := sink.newFieldErrors()
		assert.Len(t, errs, 2)
	})

	t.Run("case-insensitive dedup is path-aware", func(t *testing.T) {
		// Same name at different parents must not collapse.
		sink := newBufferingSink(nil, 0, false)
		sink.OnNewField(icebergx.Path{{Kind: icebergx.PathField, Name: "user"}}, "FOO", "x")
		sink.OnNewField(icebergx.Path{{Kind: icebergx.PathField, Name: "order"}}, "foo", "y")

		errs := sink.newFieldErrors()
		assert.Len(t, errs, 2)
	})
}

// TestParquetSinkNewFieldDedup mirrors TestBufferingSinkNewFieldDedup for the
// non-buffering parquet sink, which is used for unpartitioned writes.
func TestParquetSinkNewFieldDedup(t *testing.T) {
	t.Run("case-insensitive dedup folds case-only duplicates", func(t *testing.T) {
		// The buffer/writer state is unused by OnNewField, so a zero-value
		// sink with the caseSensitive flag set is sufficient for this test.
		sink := &parquetSink{caseSensitive: false}
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		require.Len(t, sink.newFieldErrors(), 1)
	})

	t.Run("case-sensitive dedup keeps both", func(t *testing.T) {
		sink := &parquetSink{caseSensitive: true}
		sink.OnNewField(icebergx.Path{}, "FOO", "x")
		sink.OnNewField(icebergx.Path{}, "foo", "y")

		assert.Len(t, sink.newFieldErrors(), 2)
	})
}
