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
