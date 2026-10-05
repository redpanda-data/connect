// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDatabaseNameForTest covers a test helper, not production code.
// It pins how test names map to database names on the shared containers.
func TestDatabaseNameForTest(t *testing.T) {
	t.Parallel()
	const long = "TestIntegrationMongoCDCUnresumablePositionWithoutSnapshot/"
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "plain name is unchanged",
			in:   "TestIntegrationMongoCDC",
			want: "TestIntegrationMongoCDC",
		},
		{
			name: "subtest separators, spaces, dashes and dots become underscores",
			in:   "TestFoo/with space/sub-case.1",
			want: "TestFoo_with_space_sub_case_1",
		},
		{
			name: "63 bytes is kept as is",
			in:   strings.Repeat("T", 63),
			want: strings.Repeat("T", 63),
		},
		{
			name: "long name is truncated to 63 bytes with a hash suffix",
			in:   long + "reset_opts_into_skipping_the_gap",
			want: "TestIntegrationMongoCDCUnresumablePositionWithoutSnaps_72cecfe3",
		},
		{
			name: "long name with the same prefix gets a different suffix",
			in:   long + "the_default_refuses_to_skip_the_gap",
			want: "TestIntegrationMongoCDCUnresumablePositionWithoutSnaps_d33e1d2f",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := databaseNameForTest(tt.in)
			assert.Equal(t, tt.want, got)
			assert.LessOrEqual(t, len(got), 63)
		})
	}
}
