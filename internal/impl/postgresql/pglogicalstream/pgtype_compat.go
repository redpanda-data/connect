// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import "strings"

// sanitizeTsrange strips quoting from Postgres tsrange text representations.
//
// Postgres quotes range bounds containing spaces, producing:
//
//	["2024-01-01 00:00:00","2024-12-31 00:00:00")
//
// The old pgtype.Tsrange.Scan().Value() round-trip would parse and
// re-serialize this, producing:
//
//	[2024-01-01 00:00:00,2024-12-31 00:00:00)
//
// This function replicates that behavior by stripping all double quotes.
// This is safe for tsrange because timestamp bound values never contain
// literal double quotes — they consist only of digits, dashes, colons,
// spaces, and decimal points.
//
// NOTE: This function is NOT suitable for arbitrary range types whose
// bound values may contain literal double quotes (e.g. text ranges).
// For such types, a proper range parser that handles quoting and escaping
// (like the old pgtype.ParseUntypedTextRange) would be needed.
func sanitizeTsrange(s string) string {
	return strings.ReplaceAll(s, `"`, "")
}
