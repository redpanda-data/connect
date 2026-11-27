// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"errors"
	"fmt"
	"testing"

	goora "github.com/sijms/go-ora/v2/network"
	"github.com/stretchr/testify/assert"
)

func TestIsMissingPrivilege(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "insufficient privileges", err: goora.NewOracleError(1031), want: true},
		{name: "missing privilege on 23ai", err: goora.NewOracleError(41900), want: true},
		{name: "wrapped", err: fmt.Errorf("planning chunk ranges: %w", goora.NewOracleError(41900)), want: true},
		{name: "other Oracle error", err: goora.NewOracleError(942), want: false},
		{name: "not an Oracle error", err: errors.New("connection reset by peer"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := IsMissingPrivilege(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
