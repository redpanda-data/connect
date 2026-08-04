// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// TestSchemaPatternValidation verifies that the schema field is validated
// during config parsing, before any network I/O is attempted. Success is
// asserted via newPgStreamInput returning no error - the constructor doesn't
// dial the database, so a valid pattern implies validation passed.
func TestSchemaPatternValidation(t *testing.T) {
	tests := []struct {
		pattern     string
		errContains string
	}{
		{"public", ""},
		{"tenant_*", ""},
		{"*", ""},
		{`"MySchema"`, ""},
		// Regression test: len("") == 2 used to pass the old `len(s) < 2` guard.
		// Fixed to `len(s) < 3`.
		{`""`, "invalid quoted schema identifier"},
		{"1abc", "must start with a letter"},
		{`"unclosed`, "invalid quoted schema identifier"},
		{"schema-name", "invalid character"},
		{"", "schema cannot be empty"},
		{`"quoted*"`, "wildcard"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			// Single-quoted so the pattern (which may itself contain double
			// quotes, e.g. `"MySchema"`) reaches validateSchemaPattern verbatim.
			yaml := fmt.Sprintf(`
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema: '%s'
slot_name: test_slot
tables:
  - events
`, tt.pattern)

			conf, err := newPostgresCDCConfig().ParseYAML(yaml, nil)
			require.NoError(t, err)

			mgr := service.MockResources()
			license.InjectTestService(mgr)

			_, err = newPgStreamInput(conf, mgr)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}
