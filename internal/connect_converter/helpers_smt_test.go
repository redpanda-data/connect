// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFieldPath verifies the fieldPath helper produces correct Bloblang paths.
func TestFieldPath(t *testing.T) {
	tests := []struct {
		base, name, want string
	}{
		{"this", "payload", "this.payload"},
		{"root", "payload", "root.payload"},
		{"this", "event-type", `this."event-type"`},
		{"root", "event-type", `root."event-type"`},
		{"root", "a.b-c", `root.a."b-c"`},
		{"this", "a.b.c", "this.a.b.c"},
		{"root", "123start", `root."123start"`},
		{"this", "_ok_field", "this._ok_field"},
	}
	for _, tt := range tests {
		got := fieldPath(tt.base, tt.name)
		assert.Equal(t, tt.want, got, "fieldPath(%q, %q)", tt.base, tt.name)
	}
}

// TestSMTExtractFieldHyphen verifies ExtractField with a hyphenated field name
// produces lint-valid YAML and uses the quoted path form.
func TestSMTExtractFieldHyphen(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ExtractField$Value",
	  "transforms.t.field":"event-type"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root = this."event-type"`)
}

// TestSMTInsertFieldHyphen verifies InsertField$Value with a hyphenated
// static.field name produces lint-valid YAML.
func TestSMTInsertFieldHyphen(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.t.static.field":"event-type",
	  "transforms.t.static.value":"created"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root."event-type" = "created"`)
}

// TestSMTCastHyphen verifies Cast$Value with a hyphenated field name produces
// lint-valid YAML and uses the quoted path form on both LHS and RHS.
func TestSMTCastHyphen(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.Cast$Value",
	  "transforms.t.spec":"event-type:string"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `root."event-type" = this."event-type".string()`)
}

// TestSMTReplaceFieldExcludeHyphen verifies ReplaceField with a hyphenated
// excluded field name produces lint-valid YAML.
func TestSMTReplaceFieldExcludeHyphen(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ReplaceField$Value",
	  "transforms.t.exclude":"event-type"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `root."event-type" = deleted()`)
}

// TestSMTHeaderFromMoveHyphen verifies HeaderFrom$Value with a hyphenated
// field name (move operation) produces lint-valid YAML and uses quoted paths.
func TestSMTHeaderFromMoveHyphen(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.HeaderFrom$Value",
	  "transforms.t.fields":"event-type",
	  "transforms.t.headers":"x-event-type",
	  "transforms.t.operation":"move"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	y := string(res.YAML)
	assert.Contains(t, y, `this."event-type"`)
	assert.Contains(t, y, `root."event-type" = deleted()`)
}

// TestSMTValueToKeyHyphen verifies ValueToKey with a hyphenated field name
// produces lint-valid YAML and uses the quoted path form.
func TestSMTValueToKeyHyphen(t *testing.T) {
	in := smtCarrier(`"transforms.t.type":"org.apache.kafka.connect.transforms.ValueToKey",
	  "transforms.t.fields":"event-type"`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, string(res.YAML), `meta key = this."event-type".string()`)
}
