// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTempSlotName(t *testing.T) {
	t.Run("appends random suffix", func(t *testing.T) {
		a, err := tempSlotName("my_slot")
		require.NoError(t, err)
		b, err := tempSlotName("my_slot")
		require.NoError(t, err)
		require.NotEqual(t, a, b, "two calls must produce distinct names to avoid collision")
		require.True(t, strings.HasPrefix(a, "my_slot_dsc_"))
		require.True(t, strings.HasPrefix(b, "my_slot_dsc_"))
	})

	t.Run("truncates long parent to fit pg slot name limit", func(t *testing.T) {
		// PG slot names are limited to 64 bytes. Build a parent that would
		// otherwise overflow.
		parent := strings.Repeat("x", 80)
		got, err := tempSlotName(parent)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(got), 64, "slot name must fit in postgres' 64-byte limit")
		assert.True(t, strings.Contains(got, "_dsc_"))
	})
}

func TestWatermarkKey(t *testing.T) {
	cases := []struct {
		name  string
		fqn   TableFQN
		want  string
	}{
		{
			name: "unquoted simple identifiers",
			fqn:  TableFQN{Schema: "public", Table: "orders"},
			want: "public.orders",
		},
		{
			name: "quoted case-sensitive identifiers are unquoted",
			fqn:  TableFQN{Schema: `"MySchema"`, Table: `"MyTable"`},
			want: "MySchema.MyTable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, watermarkKey(tc.fqn))
		})
	}
}

func TestStreamWatermarkSuppressionLogic(t *testing.T) {
	// Exercise the pure suppression logic on a Stream value, without spinning
	// up postgres. We construct only the fields that shouldSuppressForWatermark
	// touches.
	s := &Stream{
		tableWatermark: map[string]LSN{},
	}

	// No watermark → no suppression.
	assert.False(t, s.shouldSuppressForWatermark("public", "orders", LSN(100)))

	// Install a watermark via the same code path the discoverer uses.
	s.installDiscoveredTable(TableFQN{Schema: "public", Table: "orders"}, LSN(500))

	// Below or equal to watermark → suppress.
	assert.True(t, s.shouldSuppressForWatermark("public", "orders", LSN(100)))
	assert.True(t, s.shouldSuppressForWatermark("public", "orders", LSN(500)))

	// Above watermark → emit.
	assert.False(t, s.shouldSuppressForWatermark("public", "orders", LSN(501)))

	// Different table → not affected.
	assert.False(t, s.shouldSuppressForWatermark("public", "users", LSN(100)))

	// Quoted-identifier installs are looked up by their unquoted form,
	// matching the form pgoutput emits in RelationMessage.
	s.installDiscoveredTable(TableFQN{Schema: `"MySchema"`, Table: `"MyTable"`}, LSN(700))
	assert.True(t, s.shouldSuppressForWatermark("MySchema", "MyTable", LSN(700)))
	assert.False(t, s.shouldSuppressForWatermark("MySchema", "MyTable", LSN(701)))
}

func TestStreamKnownTableNames(t *testing.T) {
	s := &Stream{
		tables: []TableFQN{
			{Schema: "public", Table: "orders"},
			{Schema: "public", Table: "users"},
		},
		tableWatermark: map[string]LSN{},
	}
	known := s.knownTableNames()
	assert.Len(t, known, 2)
	_, hasOrders := known["public.orders"]
	_, hasUsers := known["public.users"]
	assert.True(t, hasOrders)
	assert.True(t, hasUsers)

	// installDiscoveredTable must extend the known set so the next discovery
	// tick treats the table as already-known.
	s.installDiscoveredTable(TableFQN{Schema: "public", Table: "newly_discovered"}, LSN(1))
	known = s.knownTableNames()
	_, hasNew := known["public.newly_discovered"]
	assert.True(t, hasNew)
}
