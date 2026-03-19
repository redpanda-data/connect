// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"encoding/json"
	"net/netip"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeTsrange(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "quoted timestamps",
			input: `["2024-01-01 00:00:00","2024-12-31 00:00:00")`,
			want:  `[2024-01-01 00:00:00,2024-12-31 00:00:00)`,
		},
		{
			name:  "already unquoted",
			input: `[2024-01-01 00:00:00,2024-12-31 00:00:00)`,
			want:  `[2024-01-01 00:00:00,2024-12-31 00:00:00)`,
		},
		{
			name:  "empty range",
			input: "empty",
			want:  "empty",
		},
		{
			name:  "exclusive bounds",
			input: `("2024-01-01 00:00:00","2024-12-31 00:00:00")`,
			want:  `(2024-01-01 00:00:00,2024-12-31 00:00:00)`,
		},
		{
			name:  "unbounded upper",
			input: `["2024-01-01 00:00:00",)`,
			want:  `[2024-01-01 00:00:00,)`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitizeTsrange(tc.input))
		})
	}
}

func TestInetParsing(t *testing.T) {
	// Replicate the old pgtype.Inet behavior: bare IPs get a host prefix
	// length appended (/32 for IPv4, /128 for IPv6).
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "bare IPv4",
			input: "192.168.1.1",
			want:  "192.168.1.1/32",
		},
		{
			name:  "CIDR IPv4",
			input: "192.168.1.0/24",
			want:  "192.168.1.0/24",
		},
		{
			name:  "bare IPv6",
			input: "::1",
			want:  "::1/128",
		},
		{
			name:  "CIDR IPv6",
			input: "fe80::/10",
			want:  "fe80::/10",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prefix, err := netip.ParsePrefix(tc.input)
			if err != nil {
				addr, err := netip.ParseAddr(tc.input)
				require.NoError(t, err)
				prefix = netip.PrefixFrom(addr, addr.BitLen())
			}
			assert.Equal(t, tc.want, prefix.String())
		})
	}
}

func TestInt4ArraySQLScanner(t *testing.T) {
	m := pgtype.NewMap()

	t.Run("basic array", func(t *testing.T) {
		var result []*int32
		require.NoError(t, m.SQLScanner(&result).Scan("{1,2,3,4,5}"))
		b, err := json.Marshal(result)
		require.NoError(t, err)
		assert.JSONEq(t, `[1,2,3,4,5]`, string(b))
	})

	t.Run("array with null", func(t *testing.T) {
		var result []*int32
		require.NoError(t, m.SQLScanner(&result).Scan("{1,NULL,3}"))
		b, err := json.Marshal(result)
		require.NoError(t, err)
		assert.JSONEq(t, `[1,null,3]`, string(b))
	})
}

func TestTextArraySQLScanner(t *testing.T) {
	m := pgtype.NewMap()

	t.Run("basic array", func(t *testing.T) {
		var result []*string
		require.NoError(t, m.SQLScanner(&result).Scan(`{foo,"bar baz",qux}`))
		b, err := json.Marshal(result)
		require.NoError(t, err)
		assert.JSONEq(t, `["foo","bar baz","qux"]`, string(b))
	})

	t.Run("array with null", func(t *testing.T) {
		var result []*string
		require.NoError(t, m.SQLScanner(&result).Scan(`{foo,NULL,bar}`))
		b, err := json.Marshal(result)
		require.NoError(t, err)
		assert.JSONEq(t, `["foo",null,"bar"]`, string(b))
	})
}
