// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLSNScanner(t *testing.T) {
	var lsn LSN
	lsnBuf := []byte{0x00, 0x00, 0x00, 0x2d, 0x00, 0x00, 0x04, 0xb0, 0x00, 0x03}
	lsnText := "0x0000002d000004b00003"

	require.NoError(t, lsn.Scan(lsnBuf))
	require.Equal(t, lsnText, lsn.String())

	require.Error(t, lsn.Scan(lsnText))
	require.Nil(t, lsn)
}

func TestOpTypeToString(t *testing.T) {
	tests := []struct {
		name  string
		given int
	}{
		{name: "read", given: 0},
		{name: "delete", given: 1},
		{name: "insert", given: 2},
		{name: "update_before", given: 3},
		{name: "update_after", given: 4},
		{name: "unknown(5)", given: 5},
		{name: "unknown(-1)", given: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OpType(tt.given).String()
			require.Equal(t, got, tt.name)
		})
	}
}
