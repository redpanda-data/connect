// Copyright 2026 Redpanda Data, Inc.
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

func parsePgStreamInput(t *testing.T, yaml string) (service.BatchInput, error) {
	t.Helper()
	conf, err := newPostgresCDCConfig().ParseYAML(yaml, nil)
	require.NoError(t, err)

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	return newPgStreamInput(conf, mgr)
}

// TestSchemaDefault verifies that the schema field defaults to "public" when
// left unset, matching pre-multi-schema behaviour.
func TestSchemaDefault(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.NoError(t, err)
}

// TestSchemaPatternValidation verifies that the schema_pattern field is
// validated during config parsing, before any network I/O is attempted.
// Success is asserted via newPgStreamInput returning no error - the
// constructor doesn't dial the database, so a valid pattern implies
// validation passed.
func TestSchemaPatternValidation(t *testing.T) {
	tests := []struct {
		pattern     string
		errContains string
	}{
		{"tenant_*", ""},
		{"*", ""},
		{`"MySchema"`, ""},
		// Regression test: validateSchemaPattern must accept the same unicode
		// letters/digits that sanitize.NormalizePostgresIdentifier accepts for
		// unquoted identifiers (e.g. "münchen"), not just ASCII.
		{"münchen", ""},
		{"tenant_ü*", ""},
		// Regression test: len("") == 2 used to pass the old `len(s) < 2` guard.
		// Fixed to `len(s) < 3`.
		{`""`, "invalid quoted schema identifier"},
		{"1abc", "must start with a letter"},
		{`"unclosed`, "invalid quoted schema identifier"},
		// Regression test: an unquoted pattern is matched against stored
		// schema names, not parsed as an identifier, so hyphens (invalid in
		// unquoted Postgres identifiers) must still be accepted - e.g. to
		// match a UUID-suffixed tenant schema that had to be created quoted.
		{"schema-name", ""},
		{"a0eebc99-*", ""},
		{`"quoted*"`, "wildcard"},
		{`a"b`, `must not contain '"'`},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			// Single-quoted so the pattern (which may itself contain double
			// quotes, e.g. `"MySchema"`) reaches validateSchemaPattern verbatim.
			yaml := fmt.Sprintf(`
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema_pattern: '%s'
slot_name: test_slot
tables:
  - events
`, tt.pattern)

			_, err := parsePgStreamInput(t, yaml)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestSchemaAndSchemaPatternMutuallyExclusive verifies that setting both
// schema (to a non-default value) and schema_pattern is rejected at config
// construction time.
func TestSchemaAndSchemaPatternMutuallyExclusive(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema: tenant_foo
schema_pattern: 'tenant_*'
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema and schema_pattern are mutually exclusive")
}

// TestSchemaPatternWithDefaultSchemaSucceeds verifies that setting
// schema_pattern while leaving schema untouched (at its "public" default) is
// allowed.
func TestSchemaPatternWithDefaultSchemaSucceeds(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema_pattern: 'tenant_*'
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.NoError(t, err)
}

// TestExcludeSchemasValidation verifies that each exclude_schemas entry is
// validated with the same rules as schema_pattern - validateSchemaPattern is
// reused rather than re-derived, so this exercises the same error cases
// TestSchemaPatternValidation covers, just reached through a different field.
func TestExcludeSchemasValidation(t *testing.T) {
	tests := []struct {
		pattern     string
		errContains string
	}{
		{"tenant_test", ""},
		{"tenant_test_*", ""},
		{`"MySchema"`, ""},
		{"1abc", "must start with a letter"},
		{`"unclosed`, "invalid quoted schema identifier"},
		{"schema-name", "invalid character"},
		{`"quoted*"`, "wildcard"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			// Single-quoted so the pattern (which may itself contain double
			// quotes, e.g. `"MySchema"`) reaches validateSchemaPattern verbatim.
			yaml := fmt.Sprintf(`
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema_pattern: 'tenant_*'
exclude_schemas: ['%s']
slot_name: test_slot
tables:
  - events
`, tt.pattern)

			_, err := parsePgStreamInput(t, yaml)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestExcludeSchemasRequiresSchemaPattern verifies that exclude_schemas is
// rejected at config-parse time when schema_pattern is left unset. Both
// single-exact-schema mode and FOR ALL TABLES mode (empty tables) have no
// well-defined candidate set to exclude from, so this is a hard error rather
// than a silent no-op.
func TestExcludeSchemasRequiresSchemaPattern(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
exclude_schemas: [tenant_test]
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exclude_schemas requires schema_pattern to be set")
}

// TestExcludeSchemasEmptyWithoutSchemaPatternSucceeds verifies that leaving
// exclude_schemas at its default empty list does not trip the
// requires-schema_pattern check, since there's nothing to exclude.
func TestExcludeSchemasEmptyWithoutSchemaPatternSucceeds(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.NoError(t, err)
}

// TestSchemaAcceptsUnicodeIdentifier verifies that the schema field (single
// exact-name path) still accepts unquoted unicode identifiers like
// "münchen", matching sanitize.NormalizePostgresIdentifier which is the sole
// validator on this path (see NewPgStream). schema no longer runs through
// validateSchemaPattern, so this guards against that ASCII-only validator
// regressing this path again in the future.
func TestSchemaAcceptsUnicodeIdentifier(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema: münchen
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.NoError(t, err)
}

func TestNewPgStreamInputSignalTableName(t *testing.T) {
	env := service.NewEnvironment()
	spec := newPostgresCDCConfig()

	tests := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "no signal table configured",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
`,
		},
		{
			name: "signal table distinct from tables",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
signal_table_name: rpcn_signal_table
`,
		},
		{
			name: "signal table also listed in tables",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
  - rpcn_signal_table
signal_table_name: rpcn_signal_table
`,
			errContains: `signal_table_name "rpcn_signal_table" must not also appear in tables`,
		},
		{
			name: "signal table matches tables entry under different case-folding",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
  - RPCN_SIGNAL_TABLE
signal_table_name: rpcn_signal_table
`,
			errContains: `signal_table_name "rpcn_signal_table" must not also appear in tables`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(test.conf, env)
			require.NoError(t, err)

			mgr := service.MockResources()
			license.InjectTestService(mgr)

			_, err = newPgStreamInput(pConf, mgr)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
