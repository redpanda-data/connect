// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateRoundTripsBigintPrimaryKeysExactly(t *testing.T) {
	const (
		snowflake = int64(1234567890123456789)
		justOver  = int64(1)<<53 + 1
	)

	state := &State{
		Version:      CurrentStateVersion,
		CurrentTable: &TableID{Schema: "public", Table: "events"},
		LastSentPK:   PrimaryKey{snowflake},
		MaxPK:        PrimaryKey{justOver},
	}

	encoded, err := json.Marshal(state)
	require.NoError(t, err)

	var got State
	require.NoError(t, json.Unmarshal(encoded, &got))

	assert.Equal(t, snowflake, got.LastSentPK[0])
	assert.Equal(t, justOver, got.MaxPK[0])
}

func TestStateUnmarshalPrimaryKeyElementTypes(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want any
	}{
		{
			// This case is the usual one. pgx makes an int64 or an int32
			// from a Postgres integer. Both must return as an integer and
			// not as a float.
			name: "small integer stays integral",
			raw:  `42`,
			want: int64(42),
		},
		{
			name: "negative integer",
			raw:  `-42`,
			want: int64(-42),
		},
		{
			name: "max int64",
			raw:  `9223372036854775807`,
			want: int64(9223372036854775807),
		},
		{
			// A composite key can hold text or a UUID. This defect never
			// changed such a value, and the value must not change now.
			name: "string passes through",
			raw:  `"a7b3e6e4-0000-4000-8000-000000000000"`,
			want: "a7b3e6e4-0000-4000-8000-000000000000",
		},
		{
			name: "fractional falls back to float64",
			raw:  `1.5`,
			want: 1.5,
		},
		{
			// Go has no exact integer type for a value above int64.
			// Therefore this value is not exact. This test shows that the
			// result is intentional.
			name: "beyond int64 falls back to float64",
			raw:  `18446744073709551615`,
			want: float64(18446744073709551615),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got State
			require.NoError(t, json.Unmarshal([]byte(`{"version":1,"max_pk":[`+test.raw+`]}`), &got))
			require.Len(t, got.MaxPK, 1)
			assert.Equal(t, test.want, got.MaxPK[0])
		})
	}
}

func TestStateUnmarshalCompositeAndAbsentKeys(t *testing.T) {
	var got State
	require.NoError(t, json.Unmarshal([]byte(
		`{"version":1,"done":false,"last_sent_pk":[1234567890123456789,"tenant-a"],"remaining_tables":[{"Schema":"public","Table":"orders"}]}`,
	), &got))

	assert.Equal(t, PrimaryKey{int64(1234567890123456789), "tenant-a"}, got.LastSentPK)
	assert.Nil(t, got.MaxPK, "an absent key must stay nil, not become an empty slice")
	assert.Equal(t, []TableID{{Schema: "public", Table: "orders"}}, got.RemainingTables)
}

func TestStateUnmarshalRejectsMalformedJSON(t *testing.T) {
	var got State
	require.Error(t, json.Unmarshal([]byte(`{"version":`), &got))
}

// TestStateUnmarshalVersions: a checkpoint from any supported layout must
// load, or an upgrade would restart every backfill in progress. Anything
// outside the range must be refused rather than half-read.
//
// Version 1 predates Tables, and version 2 carried a Done flag that no
// longer exists. Neither is a problem to decode: a queue that outlived them
// is still in RemainingTables.
func TestStateUnmarshalVersions(t *testing.T) {
	for _, test := range []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "version 1", raw: `{"version":1,"last_sent_pk":[42]}`},
		{name: "version 2", raw: `{"version":2,"done":true,"last_sent_pk":[42]}`},
		{name: "current version", raw: fmt.Sprintf(`{"version":%d,"last_sent_pk":[42]}`, CurrentStateVersion)},
		{name: "newer than current", raw: `{"version":4,"last_sent_pk":[42]}`, wantErr: true},
		{name: "older than supported", raw: `{"version":0,"last_sent_pk":[42]}`, wantErr: true},
		{name: "version absent", raw: `{"last_sent_pk":[42]}`, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got State
			err := json.Unmarshal([]byte(test.raw), &got)
			if test.wantErr {
				require.ErrorIs(t, err, ErrUnsupportedStateVersion)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, PrimaryKey{int64(42)}, got.LastSentPK)
		})
	}
}

// TestStateUnmarshalVersion1CarriesNoTables: version 1 has no table set, so
// a resumed coordinator knows only what its queue names.
func TestStateUnmarshalVersion1CarriesNoTables(t *testing.T) {
	var got State
	require.NoError(t, json.Unmarshal([]byte(`{"version":1,"remaining_tables":[{"schema":"public","table":"b"}]}`), &got))

	assert.Nil(t, got.Tables)
	assert.Equal(t, []TableID{{Schema: "public", Table: "b"}}, got.RemainingTables)
}
