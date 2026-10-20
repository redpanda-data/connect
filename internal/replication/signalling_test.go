// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeSignal(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		signalType string
		data       string
		wantMsg    string
		wantKnown  bool
		wantErr    bool
	}{
		{name: "logSignalWithMessage", id: "1", signalType: LogSignalType, data: `{"message":"hello"}`, wantMsg: "hello", wantKnown: true},
		{name: "logSignalBadJSON", id: "2", signalType: LogSignalType, data: `{not json`, wantErr: true},
		{name: "snapshotSignalDataNotParsed", id: "3", signalType: SnapshotSignalType, data: `not json at all`, wantKnown: true},
		{name: "unknownType", id: "4", signalType: "mystery", data: `whatever`, wantKnown: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sig, err := DecodeSignal(test.id, test.signalType, []byte(test.data))
			if test.wantErr {
				require.ErrorIs(t, err, ErrSignalRejected)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, sig)
			assert.Equal(t, test.id, sig.ID)
			assert.Equal(t, test.signalType, sig.SignalType)
			assert.Equal(t, test.wantMsg, sig.Message)
			assert.Nil(t, sig.LSN)
			assert.Equal(t, test.wantKnown, IsKnownSignalType(sig.SignalType))
		})
	}
}

func TestParseSnapshotSignal(t *testing.T) {
	tests := []struct {
		name       string
		data       string
		wantTables []string
		wantErr    bool
	}{
		{name: "valid", data: `{"tables":["a","b.c"]}`, wantTables: []string{"a", "b.c"}},
		{name: "badJSON", data: `{"tables":`, wantErr: true},
		{name: "emptyTables", data: `{"tables":[]}`, wantErr: true},
		{name: "missingTablesKey", data: `{}`, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sig, err := ParseSnapshotSignal([]byte(test.data))
			if test.wantErr {
				require.ErrorIs(t, err, ErrSignalRejected)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantTables, sig.Tables)
		})
	}
}

func TestIsKnownSignalType(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "log", in: LogSignalType, want: true},
		{name: "snapshotExecute", in: SnapshotSignalType, want: true},
		{name: "unknown", in: "other", want: false},
		{name: "empty", in: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, IsKnownSignalType(test.in))
		})
	}
}
