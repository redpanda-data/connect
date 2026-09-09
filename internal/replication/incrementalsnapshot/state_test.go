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

func TestStateUnmarshalDoneCheckpoint(t *testing.T) {
	// State returns this value after the snapshot of each table is
	// complete.
	var got State
	require.NoError(t, json.Unmarshal([]byte(`{"version":1,"done":true}`), &got))

	assert.True(t, got.Done)
	assert.Equal(t, CurrentStateVersion, got.Version)
	assert.Nil(t, got.CurrentTable)
}

func TestStateUnmarshalRejectsForeignVersion(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"newer version", `{"version":2,"last_sent_pk":[42]}`},
		{"older version", `{"version":0,"last_sent_pk":[42]}`},
		{"version absent", `{"last_sent_pk":[42]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got State
			err := json.Unmarshal([]byte(test.raw), &got)
			require.ErrorIs(t, err, ErrUnsupportedStateVersion)
		})
	}
}

func TestStateUnmarshalAcceptsCurrentVersion(t *testing.T) {
	var got State
	require.NoError(t, json.Unmarshal(fmt.Appendf(nil, `{"version":%d,"done":true}`, CurrentStateVersion), &got))
	assert.True(t, got.Done)
}
