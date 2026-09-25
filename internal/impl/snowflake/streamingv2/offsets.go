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

// syntheticTokenPrefix marks an offset token minted by the caller purely to
// give WaitUntilCommitted something to poll for when no offset_token is
// configured.
const syntheticTokenPrefix = "~rpcn-at-least-once-"

// FormatSyntheticToken renders n as a synthetic offset token. These are
// compared as strings by CompareOffsetTokens, so two properties matter:
// fixed width keeps string order equal to numeric order between synthetic
// tokens, and the leading '~' (the highest printable ASCII byte) sorts a
// synthetic token above any realistic operator token, so a channel that
// carried real tokens can safely switch to synthetic ones. n must be
// non-negative; callers seed it from wall-clock nanoseconds.
func FormatSyntheticToken(n int64) string {
	return fmt.Sprintf("%s%019d", syntheticTokenPrefix, n)
}

// IsSyntheticToken reports whether tok came from FormatSyntheticToken. A
// channel whose committed token is synthetic cannot accept real tokens as
// newer, so callers use this to refuse exactly-once dedup against it.
func IsSyntheticToken(tok string) bool {
	return strings.HasPrefix(tok, syntheticTokenPrefix)
}

// ParseSyntheticToken recovers the counter behind a synthetic token so a
// caller can resume strictly above a value already committed.
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

// MixedNumericTokens reports whether exactly one of a, b parses as an int64.
// Such a pair can only be compared as strings, which is meaningless ("5000"
// sorts below "postgres-..."), and usually means the offset_token expression
// changed on a channel that already has committed tokens.
func MixedNumericTokens(a, b string) bool {
	_, aerr := strconv.ParseInt(a, 10, 64)
	_, berr := strconv.ParseInt(b, 10, 64)
	return (aerr == nil) != (berr == nil)
}

// CompareOffsetTokens orders two offset tokens, returning -1, 0 or +1:
// numerically when both parse as int64 (a string comparison would put "10"
// before "9"), otherwise lexicographically.
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
