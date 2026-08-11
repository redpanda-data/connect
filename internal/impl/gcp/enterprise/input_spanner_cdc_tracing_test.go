// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestCDCTracingSpans verifies that the stream phase emits a tracing span via
// the connector's tracing seam (CONTRIBUTING.md §5.4.4), using an in-memory
// recording tracer. A Spanner change stream has no distinct snapshot phase.
func TestCDCTracingSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	r := &spannerCDCReader{tracer: tp.Tracer("gcp_spanner_cdc")}

	t.Run("stream phase emits a span", func(t *testing.T) {
		exporter.Reset()
		called := false
		err := r.spanned(context.Background(), traceStream, func(context.Context) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		assert.True(t, called, "wrapped stream work should run")

		spans := exporter.GetSpans()
		require.Len(t, spans, 1)
		assert.Equal(t, traceStream, spans[0].Name)
		assert.Equal(t, codes.Unset, spans[0].Status.Code)
	})

	t.Run("phase error is recorded on the span", func(t *testing.T) {
		exporter.Reset()
		boom := errors.New("boom")
		err := r.spanned(context.Background(), traceStream, func(context.Context) error {
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
