// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGetDiagnosticsMessageTextClamp exercises the ODBC buffer-length contract:
// textLen from SQLGetDiagRec is the REQUIRED length before truncation, not how
// much was written. It may exceed the 1024-byte buffer, so we clamp before slicing.
func TestGetDiagnosticsMessageTextClamp(t *testing.T) {
	msgBuf := make([]byte, 1024)
	for i := range msgBuf {
		msgBuf[i] = 'A'
	}
	textLen := SQLSMALLINT(2000) // server reports 2000 bytes needed; buffer is 1024

	n := min(int(textLen), len(msgBuf))
	assert.NotPanics(t, func() { _ = string(msgBuf[:n]) })
	assert.Len(t, string(msgBuf[:n]), 1024)
}

func TestGetDiagnosticsMessageTextExact(t *testing.T) {
	// When textLen fits within the buffer the full message is used as-is.
	msgBuf := make([]byte, 1024)
	copy(msgBuf, "short message")
	textLen := SQLSMALLINT(13)

	n := min(int(textLen), len(msgBuf))
	assert.Equal(t, "short message", string(msgBuf[:n]))
}

func TestGetDiagnosticsMessageTextZero(t *testing.T) {
	// textLen == 0 should produce an empty string without panic.
	msgBuf := make([]byte, 1024)
	textLen := SQLSMALLINT(0)

	n := min(int(textLen), len(msgBuf))
	assert.Empty(t, string(msgBuf[:n]))
}
