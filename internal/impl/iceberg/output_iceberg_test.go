// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// captureIcebergOutput parses the given extra top-level YAML on top of a minimal
// valid iceberg output config and constructs the output through
// newIcebergOutputFromConfig against a mock Resources whose logger writes into a
// buffer. It returns everything logged during construction so the two runtime
// notices wired in newIcebergOutputFromConfig can be asserted at their real
// call site (rather than only through the pure helpers they delegate to). The
// capture seam is service.NewLoggerFromSlog + MockResourcesOptUseLogger — the
// same one internal/impl/kafka's hooks_test.go uses.
func captureIcebergOutput(t *testing.T, extra string) string {
	t.Helper()

	conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    bucket: bucket
`+extra, nil)
	require.NoError(t, err)

	var buf bytes.Buffer
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	mgr := service.MockResources(service.MockResourcesOptUseLogger(logger))

	// NewRouter (and hence newIcebergOutputFromConfig) does no catalog I/O, so
	// this succeeds without a live catalog — construction is exactly where the
	// two notices fire.
	_, err = newIcebergOutputFromConfig(conf, mgr)
	require.NoError(t, err)

	return buf.String()
}

// orderingWarned / amplificationInformed key off substrings unique to each
// message (output_iceberg.go). The ordering WARNING is about correctness under
// concurrent commits; the amplification INFO is about copy-on-write write cost.
func orderingWarned(out string) bool {
	return strings.Contains(out, "concurrent batches may commit out of order")
}

func amplificationInformed(out string) bool {
	return strings.Contains(out, "write amplification bounded")
}

// TestRuntimeNoticeWiring pins the two startup log notices wired in
// newIcebergOutputFromConfig (currently 0% covered): the max_in_flight ordering
// WARNING and the copy-on-write write-amplification INFO. The pure helpers
// (mutating / cowAmplificationWarning) are unit-tested separately in
// cow_polish_test.go; this exercises the WIRING that decides whether each fires.
func TestRuntimeNoticeWiring(t *testing.T) {
	cases := []struct {
		name         string
		extra        string
		wantOrdering bool
		wantAmp      bool
	}{
		{
			// copy-on-write + mutating + concurrent: both the ordering warning
			// (correctness) and the amplification info (cost) must fire.
			name:         "cow mutating max_in_flight 4 warns and informs",
			extra:        "row_operation: upsert\nidentifier_fields: [id]\nmerge_strategy: copy-on-write\nmax_in_flight: 4\n",
			wantOrdering: true,
			wantAmp:      true,
		},
		{
			// copy-on-write + mutating + serialised: no ordering hazard, so only
			// the amplification info fires.
			name:         "cow mutating max_in_flight 1 informs only",
			extra:        "row_operation: upsert\nidentifier_fields: [id]\nmerge_strategy: copy-on-write\nmax_in_flight: 1\n",
			wantOrdering: false,
			wantAmp:      true,
		},
		{
			// merge-on-read + mutating + concurrent: the ordering warning fires,
			// but amplification is a copy-on-write-only concern so it stays silent.
			name:         "mor mutating max_in_flight 4 warns only",
			extra:        "row_operation: upsert\nidentifier_fields: [id]\nmerge_strategy: merge-on-read\nmax_in_flight: 4\n",
			wantOrdering: true,
			wantAmp:      false,
		},
		{
			// append-only (explicit insert) under copy-on-write + concurrent:
			// neither fires — a non-mutating config has no ordering hazard and no
			// amplification.
			name:         "append-only insert cow max_in_flight 4 silent",
			extra:        "row_operation: insert\nmerge_strategy: copy-on-write\nmax_in_flight: 4\n",
			wantOrdering: false,
			wantAmp:      false,
		},
		{
			// append-only via the default (row_operation unset) under
			// copy-on-write + concurrent: still silent, proving the default takes
			// the non-mutating path.
			name:         "append-only default cow max_in_flight 4 silent",
			extra:        "merge_strategy: copy-on-write\nmax_in_flight: 4\n",
			wantOrdering: false,
			wantAmp:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := captureIcebergOutput(t, tc.extra)
			assert.Equal(t, tc.wantOrdering, orderingWarned(out),
				"ordering warning firing mismatch; captured log:\n%s", out)
			assert.Equal(t, tc.wantAmp, amplificationInformed(out),
				"amplification info firing mismatch; captured log:\n%s", out)
		})
	}
}
