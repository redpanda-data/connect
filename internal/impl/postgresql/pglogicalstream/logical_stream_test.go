// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderSnapshotTasks(t *testing.T) {
	tests := []struct {
		name     string
		perTable [][]string
		want     []string
	}{
		{
			name:     "no tables",
			perTable: nil,
			want:     nil,
		},
		{
			name:     "single-task tables move ahead of chunked tables",
			perTable: [][]string{{"a1", "a2", "a3"}, {"b1"}, {"c1", "c2"}, {"d1"}},
			want:     []string{"b1", "d1", "a1", "a2", "a3", "c1", "c2"},
		},
		{
			name:     "only single-task tables keep their order",
			perTable: [][]string{{"a1"}, {"b1"}, {"c1"}},
			want:     []string{"a1", "b1", "c1"},
		},
		{
			name:     "only chunked tables keep their order",
			perTable: [][]string{{"a1", "a2"}, {"b1", "b2", "b3"}},
			want:     []string{"a1", "a2", "b1", "b2", "b3"},
		},
		{
			name:     "a table with no tasks is skipped",
			perTable: [][]string{{"a1", "a2"}, {}, {"c1"}},
			want:     []string{"c1", "a1", "a2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, orderSnapshotTasks(tt.perTable))
		})
	}
}
