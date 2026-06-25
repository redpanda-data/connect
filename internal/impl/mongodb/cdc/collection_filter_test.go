// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/confx"
)

func TestFilterMongoCollections(t *testing.T) {
	re := func(patterns ...string) []*regexp.Regexp {
		var out []*regexp.Regexp
		for _, p := range patterns {
			out = append(out, regexp.MustCompile(p))
		}
		return out
	}

	collections := []string{"orders", "order_items", "users", "audit_log"}

	tests := []struct {
		name    string
		include []*regexp.Regexp
		exclude []*regexp.Regexp
		want    []string
	}{
		{
			name: "no patterns returns all",
			want: collections,
		},
		{
			name:    "include only keeps matching collections",
			include: re(`^order`),
			want:    []string{"orders", "order_items"},
		},
		{
			name:    "exclude only drops matching collections",
			exclude: re(`^audit_`),
			want:    []string{"orders", "order_items", "users"},
		},
		{
			name:    "combined include and exclude (exclude wins)",
			include: re(`^order`),
			exclude: re(`_items$`),
			want:    []string{"orders"},
		},
		{
			name:    "include matches nothing",
			include: re(`^nonexistent$`),
			want:    []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter := &confx.RegexpFilter{Include: tc.include, Exclude: tc.exclude}
			got := filterMongoCollections(filter, collections)
			require.Equal(t, tc.want, got)
		})
	}
}
