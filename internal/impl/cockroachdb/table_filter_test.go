// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crdb

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/confx"
)

func TestFilterCRDBTables(t *testing.T) {
	re := func(patterns ...string) []*regexp.Regexp {
		var out []*regexp.Regexp
		for _, p := range patterns {
			out = append(out, regexp.MustCompile(p))
		}
		return out
	}

	tables := []string{"orders", "order_items", "users", "audit_log"}

	tests := []struct {
		name    string
		include []*regexp.Regexp
		exclude []*regexp.Regexp
		want    []string
	}{
		{
			name: "no patterns returns all",
			want: tables,
		},
		{
			name:    "include only keeps matching tables",
			include: re(`^order`),
			want:    []string{"orders", "order_items"},
		},
		{
			name:    "exclude only drops matching tables",
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
			got := filterCRDBTables(filter, tables)
			require.Equal(t, tc.want, got)
		})
	}
}
