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

	"github.com/stretchr/testify/assert"
)

func TestControlSignal_TableNames(t *testing.T) {
	tests := []struct {
		name            string
		dataCollections []string
		schema          string
		want            []string
	}{
		{
			name:            "empty data-collections returns nil",
			dataCollections: nil,
			schema:          "dbo",
			want:            nil,
		},
		{
			name:            "matching schema.table extracts table name",
			dataCollections: []string{"dbo.events"},
			schema:          "dbo",
			want:            []string{"events"},
		},
		{
			name:            "multiple entries same schema",
			dataCollections: []string{"dbo.events", "dbo.products"},
			schema:          "dbo",
			want:            []string{"events", "products"},
		},
		{
			name:            "cross-schema entry is excluded",
			dataCollections: []string{"other.events"},
			schema:          "dbo",
			want:            []string{},
		},
		{
			name:            "mixed: matching and non-matching schemas",
			dataCollections: []string{"dbo.events", "other.products"},
			schema:          "dbo",
			want:            []string{"events"},
		},
		{
			name:            "schema match is case-insensitive",
			dataCollections: []string{"DBO.events"},
			schema:          "dbo",
			want:            []string{"events"},
		},
		{
			name:            "entry without schema prefix is included",
			dataCollections: []string{"events"},
			schema:          "dbo",
			want:            []string{"events"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ControlSignal{DataCollections: tt.dataCollections}
			got := s.TableNames(tt.schema)
			assert.Equal(t, tt.want, got)
		})
	}
}
