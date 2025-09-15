// Copyright 2025 Redpanda Data, Inc.
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

package confx

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegexpFilterFiltered(t *testing.T) {
	re := func(patterns ...string) []*regexp.Regexp {
		if len(patterns) == 0 {
			return nil
		}
		var regexps []*regexp.Regexp
		for _, p := range patterns {
			if p != "" {
				regexps = append(regexps, regexp.MustCompile(p))
			}
		}
		return regexps
	}

	tests := []struct {
		name    string
		all     []string
		include []string
		exclude []string
		want    []string
	}{
		{
			name:    "nil include and exclude returns all",
			all:     []string{"a", "b", "c"},
			include: nil,
			exclude: nil,
			want:    []string{"a", "b", "c"},
		},
		{
			name:    "include only filters matching entries",
			all:     []string{"alpha", "beta", "gamma", "alp"},
			include: []string{"^al"},
			exclude: nil,
			want:    []string{"alpha", "alp"},
		},
		{
			name:    "exclude only removes matching entries",
			all:     []string{"topic-1", "test-2", "topic-3"},
			include: nil,
			exclude: []string{"^topic-"},
			want:    []string{"test-2"},
		},
		{
			name:    "include and exclude with overlap (exclude wins)",
			all:     []string{"svc.orders", "svc.users", "sys.metrics"},
			include: []string{"^svc\\."},
			exclude: []string{"users$"},
			want:    []string{"svc.orders"},
		},
		{
			name:    "empty input returns empty",
			all:     []string{},
			include: []string{"^anything$"},
			exclude: []string{"^nothing$"},
			want:    []string{},
		},
		{
			name:    "order is preserved after filtering",
			all:     []string{"b", "a", "c", "ab", "ba"},
			include: []string{"a"},
			exclude: []string{"^ab$"},
			want:    []string{"a", "ba"},
		},
		{
			name:    "exclude everything when include nil",
			all:     []string{"x", "y"},
			include: nil,
			exclude: []string{".*"},
			want:    []string{},
		},
		{
			name:    "multiple include patterns (OR logic)",
			all:     []string{"foo-1", "bar-2", "baz-3", "foo-4", "qux-5"},
			include: []string{"^foo-", "^bar-"},
			exclude: nil,
			want:    []string{"foo-1", "bar-2", "foo-4"},
		},
		{
			name:    "multiple exclude patterns",
			all:     []string{"keep-1", "drop-2", "keep-3", "skip-4", "keep-5"},
			include: nil,
			exclude: []string{"^drop-", "^skip-"},
			want:    []string{"keep-1", "keep-3", "keep-5"},
		},
		{
			name:    "multiple include and exclude patterns",
			all:     []string{"svc.orders", "svc.users", "app.orders", "app.users", "sys.metrics"},
			include: []string{"^svc\\.", "^app\\."},
			exclude: []string{"users$", "metrics$"},
			want:    []string{"svc.orders", "app.orders"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := RegexpFilter{
				Include: re(tc.include...),
				Exclude: re(tc.exclude...),
			}
			got := f.Filtered(tc.all)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseRegexpPatterns(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		wantLen  int
		wantErr  bool
	}{
		{
			name:     "empty patterns returns nil",
			patterns: nil,
			wantLen:  0,
			wantErr:  false,
		},
		{
			name:     "valid patterns",
			patterns: []string{"^foo", "bar$", ".*baz.*"},
			wantLen:  3,
			wantErr:  false,
		},
		{
			name:     "empty strings are ignored",
			patterns: []string{"^foo", "", "bar$", ""},
			wantLen:  2,
			wantErr:  false,
		},
		{
			name:     "invalid pattern returns error",
			patterns: []string{"^foo", "[invalid", "bar$"},
			wantLen:  0,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRegexpPatterns(tc.patterns)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Len(t, got, tc.wantLen)
			}
		})
	}
}
