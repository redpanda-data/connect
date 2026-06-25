// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crdb

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
// recording tracer. A CockroachDB changefeed has no distinct snapshot phase.
func TestCDCTracingSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	c := &crdbChangefeedInput{tracer: tp.Tracer("cockroachdb_changefeed")}

	t.Run("stream phase emits a span", func(t *testing.T) {
		exporter.Reset()
		called := false
		err := c.spanned(context.Background(), traceStream, func(context.Context) error {
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
		err := c.spanned(context.Background(), traceStream, func(context.Context) error {
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
