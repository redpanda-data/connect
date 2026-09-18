// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"fmt"
	"strconv"
	"strings"
)

// syntheticTokenPrefix marks an offset token this client's callers minted
// themselves -- purely to give WaitUntilCommitted something to poll for
// when the operator supplied no offset_token of their own -- rather than one
// derived from the data. See FormatSyntheticToken for why the prefix begins
// with '~'.
const syntheticTokenPrefix = "~rpcn-at-least-once-"

// FormatSyntheticToken renders n as a synthetic offset token: the prefix
// above followed by n zero-padded to the full width of an int64. Two
// properties matter, and both are load-bearing for callers that put a
// synthetic token through CompareOffsetTokens (which falls back to a plain
// string comparison for anything that isn't a bare integer, as these
// deliberately aren't):
//
//  1. Fixed width keeps string order equal to numeric order between two
//     synthetic tokens, so a later token always compares greater than an
//     earlier one -- which is what WaitUntilCommitted's "committed >= mine"
//     check needs when the last committed token is itself synthetic.
//  2. The leading '~' (0x7E, the highest printable ASCII byte) makes a
//     synthetic token compare greater than essentially any token an
//     operator's own offset_token expression could produce -- digits,
//     letters, and every common separator all sort below it. That's what
//     lets a channel previously written with real tokens be switched to
//     at-least-once delivery safely: the first synthetic token is already
//     past whatever real token Snowflake last committed, so the commit
//     wait genuinely waits for its own append instead of returning early.
//
// n must be non-negative; callers seed it from a wall-clock nanosecond
// timestamp (always positive, and 19 digits wide until the year 2262).
func FormatSyntheticToken(n int64) string {
	return fmt.Sprintf("%s%019d", syntheticTokenPrefix, n)
}

// IsSyntheticToken reports whether tok was produced by FormatSyntheticToken.
// A channel whose last committed token is synthetic was last written without
// an offset_token, and its token space is therefore incompatible with any
// real offset_token expression: every real token compares as older than the
// synthetic one (see FormatSyntheticToken), so exactly-once dedup against it
// would silently drop every row as an already-committed duplicate. Callers
// use this to refuse that situation loudly instead.
func IsSyntheticToken(tok string) bool {
	return strings.HasPrefix(tok, syntheticTokenPrefix)
}

// ParseSyntheticToken recovers the counter value FormatSyntheticToken
// rendered, so a caller finding a synthetic token already committed on a
// channel can restart its own sequence strictly above it rather than
// trusting a wall-clock seed to land there on its own. ok is false for
// anything that isn't a well-formed synthetic token.
func ParseSyntheticToken(tok string) (n int64, ok bool) {
	if !IsSyntheticToken(tok) {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(tok, syntheticTokenPrefix), 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// CompareOffsetTokens orders two offset tokens, returning -1, 0 or +1.
//
// Offset tokens are opaque strings to Snowflake; ordering is the client's
// responsibility. When both sides parse as int64 we compare numerically, which
// is what Kafka offsets need — a string comparison puts "10" before "9" and
// makes exactly-once filtering wrong from offset 10 onward. Anything that does
// not parse falls back to lexicographic comparison, preserving upstream
// behaviour for non-numeric tokens.
func CompareOffsetTokens(a, b string) int {
	ai, aerr := strconv.ParseInt(a, 10, 64)
	bi, berr := strconv.ParseInt(b, 10, 64)
	if aerr == nil && berr == nil {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	}
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
