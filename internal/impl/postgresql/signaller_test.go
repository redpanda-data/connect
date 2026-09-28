// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/replication"
)

func TestPostgresSignallerEnabled(t *testing.T) {
	s, err := newControlSignaller("dbo", "", nil)
	require.NoError(t, err)
	require.IsType(t, noopSignaller{}, s, "expected a no-op signaller when no signal table is configured")

	s, err = newControlSignaller("dbo", "rpcn_signal_table", nil)
	require.NoError(t, err)
	require.IsType(t, &postgresSignaller{}, s, "expected a postgresSignaller when a signal table is configured")
}

func TestPostgresSignallerListen(t *testing.T) {
	lsn := "0/1"

	tests := []struct {
		name        string
		event       pglogicalstream.StreamMessage
		errContains string
		want        *replication.ControlSignal
	}{
		{
			name: "non-insert operation is ignored",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.UpdateOpType,
				Schema:    "dbo",
				Table:     "rpcn_signal_table",
				Data:      map[string]any{"id": 1, "type": "log", "data": `{"message": "hello"}`},
			},
		},
		{
			name: "non-matching table is ignored",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "dbo",
				Table:     "other_table",
				Data:      map[string]any{"id": 1, "type": "log", "data": `{"message": "hello"}`},
			},
		},
		{
			name: "non-matching schema is ignored",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "other_schema",
				Table:     "rpcn_signal_table",
				Data:      map[string]any{"id": 1, "type": "log", "data": `{"message": "hello"}`},
			},
		},
		{
			name: "message data is not a map",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "dbo",
				Table:     "rpcn_signal_table",
				Data:      "not-a-map",
			},
			errContains: "expected map for",
		},
		{
			name: "data column is not a string",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "dbo",
				Table:     "rpcn_signal_table",
				Data:      map[string]any{"id": 1, "type": "log", "data": nil},
			},
			errContains: "expected string for",
		},
		{
			name: "data column is not valid JSON",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "dbo",
				Table:     "rpcn_signal_table",
				Data:      map[string]any{"id": 1, "type": "log", "data": "not-json"},
			},
			errContains: "unmarshaling control signal",
		},
		{
			name: "type column is not a string",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "dbo",
				Table:     "rpcn_signal_table",
				Data:      map[string]any{"id": 1, "type": 123, "data": `{"message": "hello"}`},
			},
			errContains: "expected string for rpcn_signal_table.type column, got int",
		},
		{
			name: "recognized log type is returned",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "dbo",
				Table:     "rpcn_signal_table",
				LSN:       &lsn,
				Data:      map[string]any{"id": 1, "type": "log", "data": `{"message": "hello"}`},
			},
			want: &replication.ControlSignal{ID: "1", SignalType: "log", LogSignal: replication.LogSignal{Message: "hello"}, LSN: []byte(lsn)},
		},
		{
			name: "unrecognized type is still returned",
			event: pglogicalstream.StreamMessage{
				Operation: pglogicalstream.InsertOpType,
				Schema:    "dbo",
				Table:     "rpcn_signal_table",
				LSN:       &lsn,
				Data:      map[string]any{"id": 2, "type": "unsupported", "data": `{"message": "hello"}`},
			},
			want: &replication.ControlSignal{ID: "2", SignalType: "unsupported", LSN: []byte(lsn)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := newControlSignaller("dbo", "rpcn_signal_table", nil)
			require.NoError(t, err)

			got, err := s.listen(&test.event)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
