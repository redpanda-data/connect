// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"math"
	"testing"
	"time"
)

// TestSyntheticTokensOrderLikeTheirCounter pins property 1 of
// FormatSyntheticToken: string order (which is all CompareOffsetTokens can
// use, since these aren't bare integers) must agree with numeric order of the
// underlying counter, including across digit-count boundaries and at the
// magnitude callers actually use (wall-clock nanoseconds).
func TestSyntheticTokensOrderLikeTheirCounter(t *testing.T) {
	now := time.Now().UnixNano()
	pairs := []struct{ a, b int64 }{
		{9, 10},
		{99, 100},
		{now, now + 1},
		{now, now + 1_000_000_000},
		{0, math.MaxInt64},
	}
	for _, p := range pairs {
		ta, tb := FormatSyntheticToken(p.a), FormatSyntheticToken(p.b)
		if got := CompareOffsetTokens(ta, tb); got != -1 {
			t.Errorf("CompareOffsetTokens(%q, %q) = %d, want -1", ta, tb, got)
		}
		if got := CompareOffsetTokens(tb, ta); got != 1 {
			t.Errorf("CompareOffsetTokens(%q, %q) = %d, want 1", tb, ta, got)
		}
		if got := CompareOffsetTokens(ta, ta); got != 0 {
			t.Errorf("CompareOffsetTokens(%q, %q) = %d, want 0", ta, ta, got)
		}
	}
}

// TestSyntheticTokensCompareAboveRealTokens pins property 2: any token an
// operator's offset_token expression would realistically produce compares
// below a synthetic one, so switching a channel from exactly-once to
// at-least-once leaves the commit wait waiting for its own append.
func TestSyntheticTokensCompareAboveRealTokens(t *testing.T) {
	synthetic := FormatSyntheticToken(1)
	for _, real := range []string{
		"0", "9", "10", "5000", "9223372036854775807",
		"postgres-0/16B3748", "mysql-bin.000001:1234", "scn-98765", "zzz",
		"ABC", "0000002a:00000138:0003", "a b&c=d",
	} {
		if got := CompareOffsetTokens(real, synthetic); got != -1 {
			t.Errorf("CompareOffsetTokens(%q, synthetic) = %d, want -1", real, got)
		}
	}
}

func TestMixedNumericTokens(t *testing.T) {
	// A token beyond int64 doesn't parse, so against a small integer it
	// counts as mixed too -- CompareOffsetTokens would fall back to strings
	// for that pair as well.
	mixed := [][2]string{{"5000", "postgres-0/16B3748"}, {"abc", "1"}, {"10", FormatSyntheticToken(1)}, {"99999999999999999999", "1"}}
	for _, p := range mixed {
		if !MixedNumericTokens(p[0], p[1]) || !MixedNumericTokens(p[1], p[0]) {
			t.Errorf("MixedNumericTokens(%q, %q) = false, want true", p[0], p[1])
		}
	}
	same := [][2]string{{"9", "10"}, {"abc", "abd"}, {"postgres-1", "postgres-2"}, {"", ""}}
	for _, p := range same {
		if MixedNumericTokens(p[0], p[1]) {
			t.Errorf("MixedNumericTokens(%q, %q) = true, want false", p[0], p[1])
		}
	}
}

func TestIsSyntheticToken(t *testing.T) {
	if !IsSyntheticToken(FormatSyntheticToken(time.Now().UnixNano())) {
		t.Error("IsSyntheticToken(FormatSyntheticToken(...)) = false, want true")
	}
	for _, real := range []string{"", "1", "postgres-1", "rpcn-at-least-once-1", "~"} {
		if IsSyntheticToken(real) {
			t.Errorf("IsSyntheticToken(%q) = true, want false", real)
		}
	}
}

func TestCompareOffsetTokensNumeric(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"9", "10", -1}, // the bug: lexicographic says +1
		{"10", "9", 1},
		{"7", "7", 0},
		{"0", "9223372036854775807", -1}, // int64 max
		{"9223372036854775807", "9223372036854775806", 1},
		{"00010", "10", 0}, // leading zeros are numerically equal
	}
	for _, c := range cases {
		if got := CompareOffsetTokens(c.a, c.b); got != c.want {
			t.Errorf("CompareOffsetTokens(%q,%q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestCompareOffsetTokensFallsBackToStringWhenNotNumeric(t *testing.T) {
	// Preserves upstream behaviour for non-numeric tokens.
	if got := CompareOffsetTokens("abc", "abd"); got != -1 {
		t.Errorf("non-numeric compare = %d, want -1", got)
	}
	if got := CompareOffsetTokens("10", "abc"); got != -1 {
		t.Errorf("mixed compare should fall back to string: got %d, want -1", got)
	}
	if got := CompareOffsetTokens("", ""); got != 0 {
		t.Errorf("empty compare = %d, want 0", got)
	}
}

func TestCompareOffsetTokensRejectsOverflowAsNonNumeric(t *testing.T) {
	// Beyond int64: must not wrap or panic. Falls back to string comparison.
	huge := "99999999999999999999"
	if got := CompareOffsetTokens(huge, "1"); got != 1 {
		t.Errorf("overflow compare = %d, want 1 (string fallback)", got)
	}
}
