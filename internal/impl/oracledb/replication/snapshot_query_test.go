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

func TestBuildKeysetPageQuery(t *testing.T) {
	const (
		baseTableRef = `"S"."T" AS OF SCN 42`
		customQuery  = `SELECT "ID", "NAME" FROM "S"."T" WHERE "ACTIVE" = 1`
	)
	tests := []struct {
		name         string
		customQuery  string
		pk           []string
		lastSeen     map[string]any
		maxInclusive map[string]any
		wantQuery    string
		wantArgs     []any
	}{
		{
			name:      "first page",
			pk:        []string{"ID"},
			wantQuery: `SELECT * FROM "S"."T" AS OF SCN 42 ORDER BY "ID" FETCH FIRST 10 ROWS ONLY`,
		},
		{
			name:      "lower bound only",
			pk:        []string{"ID"},
			lastSeen:  map[string]any{"ID": 5},
			wantQuery: `SELECT * FROM "S"."T" AS OF SCN 42 WHERE (("ID" > :1)) ORDER BY "ID" FETCH FIRST 10 ROWS ONLY`,
			wantArgs:  []any{5},
		},
		{
			name:         "upper bound only",
			pk:           []string{"ID"},
			maxInclusive: map[string]any{"ID": 9},
			wantQuery:    `SELECT * FROM "S"."T" AS OF SCN 42 WHERE (("ID" <= :1)) ORDER BY "ID" FETCH FIRST 10 ROWS ONLY`,
			wantArgs:     []any{9},
		},
		{
			name:         "both bounds",
			pk:           []string{"ID"},
			lastSeen:     map[string]any{"ID": 5},
			maxInclusive: map[string]any{"ID": 9},
			wantQuery:    `SELECT * FROM "S"."T" AS OF SCN 42 WHERE (("ID" > :1)) AND (("ID" <= :2)) ORDER BY "ID" FETCH FIRST 10 ROWS ONLY`,
			wantArgs:     []any{5, 9},
		},
		{
			name:      "composite key lower bound",
			pk:        []string{"A", "B"},
			lastSeen:  map[string]any{"A": 1, "B": 2},
			wantQuery: `SELECT * FROM "S"."T" AS OF SCN 42 WHERE (("A" > :1) OR ("A" = :2 AND "B" > :3)) ORDER BY "A", "B" FETCH FIRST 10 ROWS ONLY`,
			wantArgs:  []any{1, 1, 2},
		},
		{
			name:        "custom query first page is not wrapped",
			customQuery: customQuery,
			pk:          []string{"ID"},
			wantQuery:   customQuery + ` ORDER BY "ID" FETCH FIRST 10 ROWS ONLY`,
		},
		{
			name:        "custom query with a bound is wrapped",
			customQuery: customQuery,
			pk:          []string{"ID"},
			lastSeen:    map[string]any{"ID": 5},
			wantQuery:   `SELECT * FROM (` + customQuery + `) t WHERE (("ID" > :1)) ORDER BY "ID" FETCH FIRST 10 ROWS ONLY`,
			wantArgs:    []any{5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args := buildKeysetPageQuery(baseTableRef, tt.customQuery, tt.pk, tt.lastSeen, tt.maxInclusive, 10)

			assert.Equal(t, tt.wantQuery, query)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestIsMissingFlashbackPrivilege(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "insufficient privileges", err: goora.NewOracleError(1031), want: true},
		{name: "missing flashback privilege on 23ai", err: goora.NewOracleError(41900), want: true},
		{name: "wrapped", err: fmt.Errorf("planning chunk ranges: %w", goora.NewOracleError(41900)), want: true},
		{name: "other Oracle error", err: goora.NewOracleError(942), want: false},
		{name: "not an Oracle error", err: errors.New("connection reset by peer"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := isMissingFlashbackPrivilege(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
