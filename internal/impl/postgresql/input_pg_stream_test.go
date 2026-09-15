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

// TestSchemaIncludeValidation verifies that the schema_include field is
// validated during config parsing, before any network I/O is attempted.
// Success is asserted via newPgStreamInput returning no error - the
// constructor doesn't dial the database, so a valid pattern implies
// validation passed.
func TestSchemaIncludeValidation(t *testing.T) {
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
		// Regression test: a leading digit is not an identifier-syntax
		// violation here - the pattern is compared via ILIKE, never spliced
		// into an identifier position - so "1abc" must be as valid as any
		// other unquoted pattern. See the "9c0b4ef8-*" case below for the
		// motivating real-world scenario (a UUID-suffixed tenant schema).
		{"1abc", ""},
		{`"unclosed`, "invalid quoted schema identifier"},
		// Regression test: an unquoted pattern is matched against stored
		// schema names, not parsed as an identifier, so hyphens (invalid in
		// unquoted Postgres identifiers) must still be accepted - e.g. to
		// match a UUID-suffixed tenant schema that had to be created quoted.
		{"schema-name", ""},
		{"a0eebc99-*", ""},
		// Regression test: most UUIDs begin with a hex digit, so a tenant
		// schema named e.g. "9c0b4ef8-bb6d-6bb9-bd38-0a11a0eebc99" (created
		// quoted, per the a0eebc99-* case above) must be matchable by an
		// unquoted glob starting with a digit - the pattern is compared via
		// ILIKE, never spliced into an identifier position, so there's no
		// syntactic reason to require a letter/underscore/'*' lead-in.
		{"9c0b4ef8-*", ""},
		{`"quoted*"`, "wildcard"},
		{`a"b`, `must not contain '"'`},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			// Single-quoted so the pattern (which may itself contain double
			// quotes, e.g. `"MySchema"`) reaches validateSchemaPattern verbatim.
			yaml := fmt.Sprintf(`
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema_include: '%s'
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

// TestSchemaAndSchemaIncludeMutuallyExclusive verifies that setting both
// schema (to a non-default value) and schema_include is rejected at config
// construction time.
func TestSchemaAndSchemaIncludeMutuallyExclusive(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema: tenant_foo
schema_include: 'tenant_*'
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema and schema_include are mutually exclusive")
}

// TestSchemaIncludeWithDefaultSchemaSucceeds verifies that setting
// schema_include while leaving schema untouched (at its "public" default) is
// allowed.
func TestSchemaIncludeWithDefaultSchemaSucceeds(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema_include: 'tenant_*'
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.NoError(t, err)
}

// TestSchemaExcludeValidation verifies that each schema_exclude entry is
// validated with the same rules as schema_include - validateSchemaPattern is
// reused rather than re-derived, so this exercises the same error cases
// TestSchemaIncludeValidation covers, just reached through a different field.
func TestSchemaExcludeValidation(t *testing.T) {
	tests := []struct {
		pattern     string
		errContains string
	}{
		{"tenant_test", ""},
		{"tenant_test_*", ""},
		{`"MySchema"`, ""},
		{"1abc", ""},
		{`"unclosed`, "invalid quoted schema identifier"},
		{"schema-name", ""},
		{`"quoted*"`, "wildcard"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			// Single-quoted so the pattern (which may itself contain double
			// quotes, e.g. `"MySchema"`) reaches validateSchemaPattern verbatim.
			yaml := fmt.Sprintf(`
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema_include: 'tenant_*'
schema_exclude: ['%s']
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

// TestSchemaExcludeRequiresSchemaInclude verifies that schema_exclude is
// rejected at config-parse time when schema_include is left unset. Both
// single-exact-schema mode and FOR ALL TABLES mode (empty tables) have no
// well-defined candidate set to exclude from, so this is a hard error rather
// than a silent no-op.
func TestSchemaExcludeRequiresSchemaInclude(t *testing.T) {
	yaml := `
dsn: postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable
schema_exclude: [tenant_test]
slot_name: test_slot
tables:
  - events
`
	_, err := parsePgStreamInput(t, yaml)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema_exclude requires schema_include to be set")
}

// TestSchemaExcludeEmptyWithoutSchemaIncludeSucceeds verifies that leaving
// schema_exclude at its default empty list does not trip the
// requires-schema_include check, since there's nothing to exclude.
func TestSchemaExcludeEmptyWithoutSchemaIncludeSucceeds(t *testing.T) {
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
