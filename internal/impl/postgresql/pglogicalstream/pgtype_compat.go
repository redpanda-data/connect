// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import "strings"

// sanitizeTsrange strips escaped quotes from Postgres tsrange text
// representations. Postgres may return ranges like:
//
//	["2024-01-01 00:00:00","2024-12-31 00:00:00")
//
// The old pgtype.Tsrange.Scan().Value() round-trip would parse and
// re-serialize this, producing:
//
//	[2024-01-01 00:00:00,2024-12-31 00:00:00)
//
// This function replicates that behavior without the old pgtype dependency.
func sanitizeTsrange(s string) string {
	return strings.ReplaceAll(s, `"`, "")
}
