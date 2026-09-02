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

	"github.com/stretchr/testify/require"
)

func TestTxnBoundaryObserve(t *testing.T) {
	var b txnBoundary

	// Sequence AAABBC: the last complete transaction only advances when the
	// LSN changes, and always lags one transaction behind the current row.
	observations := []struct {
		lsn          string
		lastComplete string // "" = no complete transaction yet
	}{
		{"A", ""},
		{"A", ""},
		{"A", ""},
		{"B", "A"},
		{"B", "A"},
		{"C", "B"},
	}
	for i, o := range observations {
		got := b.Observe(LSN(o.lsn))
		if o.lastComplete == "" {
			require.Emptyf(t, got, "observation %d (lsn %s)", i, o.lsn)
		} else {
			require.Equalf(t, o.lastComplete, string(got), "observation %d (lsn %s)", i, o.lsn)
		}
	}
}
