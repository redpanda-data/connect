// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/replication"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestDB2CDCInputConfigParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		configYAML  string
		errContains string
	}{
		{
			name: "valid minimal config",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES"]
`,
		},
		{
			name: "valid config with all fields",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES", "ORDERS"]
cdc_schema: "ASNCDC"
snapshot_mode: never
checkpoint_cache_table_name: "MYSCHEMA.MY_CHECKPOINT"
`,
		},
		{
			name: "schema with special chars rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2'; DROP TABLE FOO; --"
tables: ["EMPLOYEES"]
`,
			errContains: "invalid characters",
		},
		{
			name: "cdc_schema with special chars rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES"]
cdc_schema: "ASN'; DROP TABLE FOO; --"
`,
			errContains: "invalid characters",
		},
		{
			name: "checkpoint_cache_table_name with special chars rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES"]
checkpoint_cache_table_name: "RPCN.CDC'; DROP TABLE FOO"
`,
			errContains: "invalid characters",
		},
		{
			name: "lowercase schema accepted (normalized to uppercase)",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "db2admin"
tables: ["EMPLOYEES"]
`,
		},
		{
			name: "empty tables rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: []
`,
			errContains: "either tables or table_include_regex must be specified",
		},
		{
			name: "snapshot_max_batch_size zero rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES"]
snapshot_max_batch_size: 0
`,
			errContains: "snapshot_max_batch_size",
		},
		{
			name: "poll_batch_size zero rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES"]
poll_batch_size: 0
`,
			errContains: "poll_batch_size",
		},
	}

	spec := db2CDCConfigSpec()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := service.NewEnvironment()
			conf, err := spec.ParseYAML(tc.configYAML, env)

			var initErr error
			if err == nil {
				mgr := conf.Resources()
				license.InjectTestService(mgr)
				_, initErr = newDB2CDCInput(conf, mgr)
			}

			if tc.errContains == "" {
				require.NoError(t, err, "config parse error")
				require.NoError(t, initErr, "init error")
			} else {
				if err != nil {
					require.Contains(t, err.Error(), tc.errContains)
				} else {
					require.Error(t, initErr)
					require.Contains(t, initErr.Error(), tc.errContains)
				}
			}
		})
	}
}

func TestDB2CDCConfigLinting(t *testing.T) {
	t.Parallel()

	linter := service.NewEnvironment().NewComponentConfigLinter()

	tests := []struct {
		name    string
		conf    string
		lintErr string
	}{
		{
			name: "valid config",
			conf: `
db2_cdc:
  dsn: "DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
  schema: "DB2ADMIN"
  tables: ["EMPLOYEES"]
`,
		},
		{
			name: "unknown field",
			conf: `
db2_cdc:
  dsn: "DATABASE=SAMPLE;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
  schema: "DB2ADMIN"
  tables: ["EMPLOYEES"]
  unknown_field: "foo"
`,
			lintErr: "unknown_field",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lints, err := linter.LintInputYAML([]byte(tc.conf))
			require.NoError(t, err)
			if tc.lintErr != "" {
				found := false
				for _, l := range lints {
					if strings.Contains(l.Error(), tc.lintErr) {
						found = true
						break
					}
				}
				assert.True(t, found, "expected lint containing %q, got: %v", tc.lintErr, lints)
			} else {
				assert.Empty(t, lints)
			}
		})
	}
}

func TestIsValidDB2Identifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		valid bool
	}{
		{"DB2ADMIN", true},
		{"EMPLOYEES", true},
		{"MY_TABLE_123", true},
		{"ASNCDC", true},
		{"", false},
		{"db2admin", false}, // lowercase not allowed (input must be pre-uppercased)
		{"MY-TABLE", false}, // hyphen not allowed
		{"MY TABLE", false}, // space not allowed
		{"'; DROP TABLE T; --", false},
		{"SCHEMA.TABLE", false}, // dot not allowed (use validateQualifiedIdentifier for qualified names)
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.valid, isValidDB2Identifier(tc.input))
		})
	}
}

func TestValidateQualifiedIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		wantErr bool
	}{
		{"EMPLOYEES", false},
		{"RPCN.CDC_CHECKPOINT", false},
		{"MY_SCHEMA.MY_TABLE_123", false},
		{"", true},
		{"SCHEMA.'; DROP TABLE T", true},
		{"'; DROP TABLE T", true},
		{"SCHEMA.TABLE.EXTRA", true}, // second part "TABLE.EXTRA" contains a dot → invalid
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			err := validateQualifiedIdentifier(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestIsValidDB2IdentifierEdgeCases covers boundary values not exercised by the
// table-driven test above: single-character identifiers, numeric-only names,
// and underscore-only/leading/trailing underscore forms.
func TestIsValidDB2IdentifierEdgeCases(t *testing.T) {
	t.Parallel()

	// Single uppercase letter is valid.
	assert.True(t, isValidDB2Identifier("A"))
	// Single digit is invalid — DB2 unquoted identifiers cannot start with a digit.
	assert.False(t, isValidDB2Identifier("1"), "identifier starting with digit must be rejected")
	// Underscore only is valid.
	assert.True(t, isValidDB2Identifier("_"))
	// Leading underscore is valid.
	assert.True(t, isValidDB2Identifier("_TABLE"))
	// Trailing underscore is valid.
	assert.True(t, isValidDB2Identifier("TABLE_"))
	// Mixed digits and letters is valid.
	assert.True(t, isValidDB2Identifier("T1_A2"))
	// Lowercase must be rejected — callers must upper-case before passing.
	assert.False(t, isValidDB2Identifier("table"))
	// Empty string is always rejected.
	assert.False(t, isValidDB2Identifier(""))
}

// TestValidateQualifiedIdentifierEdgeCases covers dotted-name forms and
// verifies the two-part limit: SCHEMA.TABLE is valid; SCHEMA.TABLE.EXTRA is
// not because the second segment "TABLE.EXTRA" contains a dot.
func TestValidateQualifiedIdentifierEdgeCases(t *testing.T) {
	t.Parallel()

	// Unqualified (no dot) is valid.
	assert.NoError(t, validateQualifiedIdentifier("MY_TABLE"))
	// Fully qualified with schema is valid.
	assert.NoError(t, validateQualifiedIdentifier("MYSCHEMA_CDC.MY_CHECKPOINT"))
	// Three parts must be rejected — the second SplitN segment contains a dot.
	assert.Error(t, validateQualifiedIdentifier("A.B.C"))
	// Empty string must be rejected.
	assert.Error(t, validateQualifiedIdentifier(""))
	// Valid schema but invalid table part.
	assert.Error(t, validateQualifiedIdentifier("SCHEMA.bad-table"))
	// Valid schema, valid uppercase table.
	assert.NoError(t, validateQualifiedIdentifier("SCHEMA.TABLE_123"))
}

func TestTableRegexFilterConfig(t *testing.T) {
	t.Parallel()

	spec := db2CDCConfigSpec()

	tests := []struct {
		name        string
		configYAML  string
		errContains string
	}{
		{
			name: "include_regex accepts matching tables",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
table_include_regex: ["^EMP"]
`,
		},
		{
			name: "tables empty with include_regex is valid",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
table_include_regex: ["^ORDERS"]
`,
		},
		{
			name: "both tables and exclude_regex is valid",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES", "ORDERS_AUDIT"]
table_exclude_regex: ["_AUDIT$"]
`,
		},
		{
			name: "invalid include_regex rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
table_include_regex: ["[invalid"]
`,
			errContains: "table_include_regex",
		},
		{
			name: "invalid exclude_regex rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
tables: ["EMPLOYEES"]
table_exclude_regex: ["[bad"]
`,
			errContains: "table_exclude_regex",
		},
		{
			name: "neither tables nor include_regex rejected",
			configYAML: `
dsn: "DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=secret"
schema: "DB2ADMIN"
`,
			errContains: "either tables or table_include_regex must be specified",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := service.NewEnvironment()
			conf, err := spec.ParseYAML(tc.configYAML, env)

			var initErr error
			if err == nil {
				mgr := conf.Resources()
				license.InjectTestService(mgr)
				_, initErr = newDB2CDCInput(conf, mgr)
			}

			if tc.errContains == "" {
				require.NoError(t, err, "config parse error")
				require.NoError(t, initErr, "init error")
			} else {
				if err != nil {
					require.Contains(t, err.Error(), tc.errContains)
				} else {
					require.Error(t, initErr)
					require.Contains(t, initErr.Error(), tc.errContains)
				}
			}
		})
	}
}

func TestParseSnapshotSignalTablesDeduplication(t *testing.T) {
	t.Parallel()
	tables := parseSnapshotSignalTables(`{"data-collections":["DB2INST1.ORDERS","DB2INST1.ORDERS","DB2INST1.PRODUCTS"]}`, "DB2INST1", nil)
	require.Equal(t, []string{"ORDERS", "PRODUCTS"}, tables)
}

func TestParseSnapshotSignalTablesFilterPending(t *testing.T) {
	t.Parallel()
	d := &db2CDCInput{
		pendingIncrementalCtx: &replication.IncrementalSnapshotContext{
			Tables: []replication.IncrementalTable{
				{Schema: "DB2INST1", Name: "ORDERS"},
			},
			LastEmittedPK: map[string][]any{},
			MaxPK:         map[string][]any{},
		},
		schema: "DB2INST1",
	}
	tables := d.filterPendingTables([]string{"ORDERS", "PRODUCTS", "SHIPMENTS"})
	assert.Equal(t, []string{"PRODUCTS", "SHIPMENTS"}, tables,
		"ORDERS should be filtered out because it is already in pendingIncrementalCtx")
}
