// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/confx"
)

func TestSpannerIncludeTable(t *testing.T) {
	re := func(patterns ...string) []*regexp.Regexp {
		var out []*regexp.Regexp
		for _, p := range patterns {
			out = append(out, regexp.MustCompile(p))
		}
		return out
	}

	tables := []string{"Singers", "Songs", "Albums", "AuditLog"}

	filterFor := func(include, exclude []*regexp.Regexp) spannerCDCInputConfig {
		return spannerCDCInputConfig{tablesFilter: &confx.RegexpFilter{Include: include, Exclude: exclude}}
	}

	tests := []struct {
		name string
		conf spannerCDCInputConfig
		want []string
	}{
		{
			name: "nil filter emits all",
			conf: spannerCDCInputConfig{},
			want: tables,
		},
		{
			name: "no patterns emits all",
			conf: filterFor(nil, nil),
			want: tables,
		},
		{
			name: "include only keeps matching tables",
			conf: filterFor(re(`^S`), nil),
			want: []string{"Singers", "Songs"},
		},
		{
			name: "exclude only drops matching tables",
			conf: filterFor(nil, re(`^Audit`)),
			want: []string{"Singers", "Songs", "Albums"},
		},
		{
			name: "combined include and exclude (exclude wins)",
			conf: filterFor(re(`s$`), re(`^Album`)),
			want: []string{"Singers", "Songs"},
		},
		{
			name: "include matches nothing",
			conf: filterFor(re(`^Nonexistent$`), nil),
			want: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]string, 0, len(tables))
			for _, tbl := range tables {
				if tc.conf.includeTable(tbl) {
					got = append(got, tbl)
				}
			}
			require.Equal(t, tc.want, got)
		})
	}
}
