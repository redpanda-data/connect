// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforcehttp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInjectGraphQLCursor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		query  string
		cursor string
		want   string
	}{
		{
			name:   "empty cursor returns query unchanged",
			query:  `{ Account(first: 10) { edges { node { Id } } } }`,
			cursor: "",
			want:   `{ Account(first: 10) { edges { node { Id } } } }`,
		},
		{
			name:   "inject into existing (first: N) args",
			query:  `{ Account(first: 10) { edges { node { Id } } } }`,
			cursor: "abc123",
			want:   `{ Account(first: 10, after: "abc123") { edges { node { Id } } } }`,
		},
		{
			name:   "inject before PascalCase object when no args present",
			query:  `{ Account { edges { node { Id } } } }`,
			cursor: "xyz789",
			want:   `{ Account(after: "xyz789") { edges { node { Id } } } }`,
		},
		{
			name:   "only first PascalCase object gets args injected",
			query:  `{ Account { edges { node { Id Contact { Name } } } } }`,
			cursor: "page2",
			want:   `{ Account(after: "page2") { edges { node { Id Contact { Name } } } } }`,
		},
		{
			name:   "no PascalCase object and no (first: N) returns query unchanged",
			query:  `{ edges { node { id } } }`,
			cursor: "abc",
			want:   `{ edges { node { id } } }`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := injectGraphQLCursor(tc.query, tc.cursor)
			assert.Equal(t, tc.want, got)
		})
	}
}
