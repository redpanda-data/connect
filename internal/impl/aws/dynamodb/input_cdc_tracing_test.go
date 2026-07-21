// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestCDCTracingSpans verifies that the snapshot and stream phases emit tracing
// spans via the connector's tracing seam (CONTRIBUTING.md §5.4.4), using an
// in-memory recording tracer.
func TestCDCTracingSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	d := &dynamoDBCDCInput{tracer: tp.Tracer("aws_dynamodb_cdc")}

	t.Run("snapshot phase emits a span", func(t *testing.T) {
		exporter.Reset()
		called := false
		err := d.spanned(context.Background(), traceSnapshot, func(context.Context) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, called, "wrapped snapshot work should run")

		spans := exporter.GetSpans()
		require.Len(t, spans, 1)
		assert.Equal(t, traceSnapshot, spans[0].Name)
		assert.Equal(t, codes.Unset, spans[0].Status.Code)
	})

	t.Run("stream phase emits a span", func(t *testing.T) {
		exporter.Reset()
		err := d.spanned(context.Background(), traceStream, func(context.Context) error {
			return nil
		})
		require.NoError(t, err)

		spans := exporter.GetSpans()
		require.Len(t, spans, 1)
		assert.Equal(t, traceStream, spans[0].Name)
	})

	t.Run("phase error is recorded on the span", func(t *testing.T) {
		exporter.Reset()
		boom := errors.New("boom")
		err := d.spanned(context.Background(), traceStream, func(context.Context) error {
			return boom
		})
		require.ErrorIs(t, err, boom)

		spans := exporter.GetSpans()
		require.Len(t, spans, 1)
		assert.Equal(t, codes.Error, spans[0].Status.Code)
		require.Len(t, spans[0].Events, 1)
		assert.Equal(t, "exception", spans[0].Events[0].Name)
	})
}

// TestCDCTracingLiveCheckpointCommitSpan verifies that the live checkpoint
// commit during normal streaming (RecordBatcher.AckMessages, not just the flush
// on close) emits an aws_dynamodb_cdc.checkpoint_commit span.
func TestCDCTracingLiveCheckpointCommitSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	batcher := NewRecordBatcher(10000, 1000, service.MockResources().Logger())
	batcher.tracer = tp.Tracer("aws_dynamodb_cdc")

	checkpointer := &mockCheckpointer{checkpointLimit: 5}

	// Ack enough contiguous messages to cross the checkpoint limit, which
	// triggers the live commit path.
	batch1 := createTestMessages(3, "shard-001", 0)
	batch2 := createTestMessages(3, "shard-001", 3)
	batcher.AddMessages(batch1, "shard-001")
	batcher.AddMessages(batch2, "shard-001")
	require.NoError(t, batcher.AckMessages(context.Background(), checkpointer, batch1))
	require.NoError(t, batcher.AckMessages(context.Background(), checkpointer, batch2))
	require.Equal(t, 1, checkpointer.calls(), "expected one live checkpoint commit")

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, traceCheckpointCommit, spans[0].Name)
}
