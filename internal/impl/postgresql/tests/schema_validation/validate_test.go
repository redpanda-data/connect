// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

// Package schema_validation tests postgres_cdc schema pattern validation
// through the public service config API. No database connection is required:
// invalid schemas are rejected during stream construction (before any network
// I/O), so stream.Run returns synchronously for the invalid-schema cases.
package schema_validation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure" // registers none tracer and other built-ins

	"github.com/redpanda-data/connect/v4/internal/license"
	_ "github.com/redpanda-data/connect/v4/internal/impl/postgresql" // registers postgres_cdc
)

// postgresStream builds a postgres_cdc stream with the given YAML schema value
// (the raw fragment that appears after "schema: " in YAML) and injects a test
// enterprise license. Returns the stream ready to run.
func postgresStream(t *testing.T, schemaYAML string) *service.Stream {
	t.Helper()
	yaml := fmt.Sprintf(`
postgres_cdc:
  dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
  schema: %s
  slot_name: test_slot
  tables:
    - events
`, schemaYAML)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetLoggerYAML(`level: ERROR`))
	require.NoError(t, sb.AddInputYAML(yaml))
	require.NoError(t, sb.AddBatchConsumerFunc(func(_ context.Context, _ service.MessageBatch) error {
		return nil
	}))

	stream, err := sb.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	return stream
}

// TestInvalidSchemaPatterns verifies that invalid schema values are rejected
// during stream construction — before any database connection is attempted.
// stream.Run returns synchronously (no goroutine needed) when the constructor
// fails.
func TestInvalidSchemaPatterns(t *testing.T) {
	tests := []struct {
		name       string
		schemaYAML string
		wantErrMsg string
	}{
		{
			// Regression test: len("") == 2 used to pass the old `len(s) < 2`
			// guard. Fixed to `len(s) < 3`.
			name:       "empty quoted identifier",
			schemaYAML: `'""'`,
			wantErrMsg: "invalid schema",
		},
		{
			name:       "digit-first unquoted pattern",
			schemaYAML: `"1abc"`,
			wantErrMsg: "invalid schema",
		},
		{
			name:       "unterminated quoted identifier",
			schemaYAML: `'"unclosed'`,
			wantErrMsg: "invalid schema",
		},
		{
			name:       "hyphen in unquoted pattern",
			schemaYAML: `schema-name`,
			wantErrMsg: "invalid schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := postgresStream(t, tt.schemaYAML)

			// Run returns synchronously when the input constructor fails —
			// no timeout context needed, but use one as a safety net.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := stream.Run(ctx)
			require.Error(t, err, "expected stream construction to fail")
			assert.Contains(t, err.Error(), tt.wantErrMsg,
				"error should indicate schema validation failure")
		})
	}
}

// TestValidSchemaPatterns verifies that valid schema values pass construction
// and are only rejected later (at DB-connect time). We run the stream briefly
// and confirm no "invalid schema" error surfaces.
func TestValidSchemaPatterns(t *testing.T) {
	tests := []struct {
		name       string
		schemaYAML string
	}{
		{name: "exact unquoted schema", schemaYAML: `public`},
		{name: "glob pattern", schemaYAML: `tenant_*`},
		{name: "wildcard", schemaYAML: `"*"`},
		{name: "quoted exact identifier", schemaYAML: `'"MySchema"'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := postgresStream(t, tt.schemaYAML)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- stream.Run(ctx) }()

			select {
			case err := <-errCh:
				// Stream stopped before timeout — must not be a schema error.
				if err != nil {
					assert.NotContains(t, err.Error(), "invalid schema",
						"valid schema %q should not trigger schema validation error", tt.schemaYAML)
				}
			case <-ctx.Done():
				// Stream is still running after timeout — constructor succeeded,
				// stream is attempting DB connection. This is the expected path.
				_ = stream.StopWithin(2 * time.Second)
			}
		})
	}
}
