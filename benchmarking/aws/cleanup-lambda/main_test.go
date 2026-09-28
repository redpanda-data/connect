// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTTL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{name: "documented default", raw: "4", want: 4 * time.Hour},
		{name: "fractional hours", raw: "1.5", want: 90 * time.Minute},
		{name: "empty must hard-fail, not fall back", raw: "", wantErr: true},
		{name: "duration syntax must hard-fail, not fall back", raw: "4h", wantErr: true},
		{name: "zero would reap everything immediately", raw: "0", wantErr: true},
		{name: "negative", raw: "-1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTTL(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
