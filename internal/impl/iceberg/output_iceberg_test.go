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
	"time"

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
    region: us-east-1
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

func cleanupDisabledInformed(out string) bool {
	return strings.Contains(out, "cleanup_on_failure is disabled")
}

// TestParseCommitConfigCleanupOnFailure pins the `commit.cleanup_on_failure`
// plumbing, including the deliberate polarity flip: the YAML field is positively
// named and defaults to true, while CommitConfig carries the negation so a
// zero-value CommitConfig keeps cleanup enabled.
func TestParseCommitConfigCleanupOnFailure(t *testing.T) {
	cases := []struct {
		name        string
		commitYAML  string
		wantDisable bool
	}{
		{
			// No `commit` object at all: parseCommitConfig returns its own
			// defaults without consulting the field.
			name:        "commit object absent keeps cleanup enabled",
			commitYAML:  "",
			wantDisable: false,
		},
		{
			// `commit` present but the field omitted: the field default (true)
			// applies.
			name:        "field absent defaults to cleanup enabled",
			commitYAML:  "commit:\n  max_retries: 2\n",
			wantDisable: false,
		},
		{
			name:        "explicit true keeps cleanup enabled",
			commitYAML:  "commit:\n  cleanup_on_failure: true\n",
			wantDisable: false,
		},
		{
			name:        "explicit false disables cleanup",
			commitYAML:  "commit:\n  cleanup_on_failure: false\n",
			wantDisable: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    region: us-east-1
`+tc.commitYAML, nil)
			require.NoError(t, err)

			cfg, err := parseCommitConfig(conf)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDisable, cfg.DisableCleanupOnFailure)
		})
	}
}

// TestParseCommitConfigMaxSnapshotAge pins the `commit.max_snapshot_age`
// default: zero, so expiry is opt-in, whether the commit block is absent or
// the field is. The "field absent" case goes through the spec's Default
// string.
func TestParseCommitConfigMaxSnapshotAge(t *testing.T) {
	cases := []struct {
		name       string
		commitYAML string
		want       time.Duration
		wantErr    string
	}{
		{
			name:       "commit object absent",
			commitYAML: "",
			want:       0,
		},
		{
			name:       "field absent",
			commitYAML: "commit:\n  max_retries: 2\n",
			want:       0,
		},
		{
			name:       "explicit zero",
			commitYAML: "commit:\n  max_snapshot_age: 0s\n",
			want:       0,
		},
		{
			name:       "at the floor",
			commitYAML: "commit:\n  max_snapshot_age: 1m\n",
			want:       time.Minute,
		},
		{
			// The lint rule rejects this too, but lint can be skipped, and
			// a sub-minute age is the multi-writer rewrite case.
			name:       "below the floor is rejected",
			commitYAML: "commit:\n  max_snapshot_age: 30s\n",
			wantErr:    "max_snapshot_age must be zero or at least 1m",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    region: us-east-1
`+tc.commitYAML, nil)
			require.NoError(t, err)

			cfg, err := parseCommitConfig(conf)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.MaxSnapshotAge)
		})
	}
}

// TestParseCommitConfigMaintenanceInterval pins the `commit.maintenance_interval`
// default to the constant the maintenance pass uses, and the positive-value
// check that holds when lint is skipped.
func TestParseCommitConfigMaintenanceInterval(t *testing.T) {
	cases := []struct {
		name       string
		commitYAML string
		want       time.Duration
		wantErr    string
	}{
		{name: "commit object absent", commitYAML: "", want: defaultMaintenanceInterval},
		{name: "field absent", commitYAML: "commit:\n  max_retries: 2\n", want: defaultMaintenanceInterval},
		{name: "explicit value", commitYAML: "commit:\n  maintenance_interval: 1h\n", want: time.Hour},
		{name: "zero is rejected", commitYAML: "commit:\n  maintenance_interval: 0s\n", wantErr: "maintenance_interval must be a positive duration"},
		{name: "negative is rejected", commitYAML: "commit:\n  maintenance_interval: -1m\n", wantErr: "maintenance_interval must be a positive duration"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    region: us-east-1
`+tc.commitYAML, nil)
			require.NoError(t, err)

			cfg, err := parseCommitConfig(conf)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.MaintenanceInterval)
		})
	}
}

// TestParseCommitConfigManifestMergeEnabled pins the
// `commit.manifest_merge_enabled` default: the output does not stamp the
// manifest-merge property unless the config turns it on.
func TestParseCommitConfigManifestMergeEnabled(t *testing.T) {
	cases := []struct {
		name       string
		commitYAML string
		want       bool
	}{
		{
			name:       "commit object absent",
			commitYAML: "",
			want:       false,
		},
		{
			name:       "field absent",
			commitYAML: "commit:\n  max_retries: 2\n",
			want:       false,
		},
		{
			name:       "explicit value",
			commitYAML: "commit:\n  manifest_merge_enabled: true\n",
			want:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    region: us-east-1
`+tc.commitYAML, nil)
			require.NoError(t, err)

			cfg, err := parseCommitConfig(conf)
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.ManifestMergeEnabled)
		})
	}
}

// TestParseSchemaEvolutionConfigTableLocationAbsent pins that an absent
// `schema_evolution.table_location` leaves the location to the catalog.
func TestParseSchemaEvolutionConfigTableLocationAbsent(t *testing.T) {
	conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    region: us-east-1
`, nil)
	require.NoError(t, err)

	cfg, err := parseSchemaEvolutionConfig(conf)
	require.NoError(t, err)
	assert.Nil(t, cfg.TableLocation)
}

// TestStorageBlocksLint pins that a config can set at most one storage backend.
// The parser reads only the first block it finds, so a second block must be a
// lint error and not be silently ignored.
func TestStorageBlocksLint(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()
	base := `
iceberg:
  catalog:
    url: http://localhost:8181/api/catalog
  namespace: ns
  table: t
`
	cases := []struct {
		name     string
		storage  string
		wantLint bool
	}{
		{
			name:     "no storage block",
			storage:  "",
			wantLint: false,
		},
		{
			name:     "empty storage block",
			storage:  "  storage: {}\n",
			wantLint: false,
		},
		{
			name:     "aws_s3 only",
			storage:  "  storage:\n    aws_s3:\n      bucket: b\n",
			wantLint: false,
		},
		{
			name:     "gcp_cloud_storage only",
			storage:  "  storage:\n    gcp_cloud_storage:\n      bucket: b\n",
			wantLint: false,
		},
		{
			name:     "azure_blob_storage only",
			storage:  "  storage:\n    azure_blob_storage:\n      storage_account: a\n      container: c\n",
			wantLint: false,
		},
		{
			name:     "aws_s3 and gcp_cloud_storage",
			storage:  "  storage:\n    aws_s3:\n      bucket: b\n    gcp_cloud_storage:\n      bucket: b\n",
			wantLint: true,
		},
		{
			name:     "all three",
			storage:  "  storage:\n    aws_s3:\n      bucket: b\n    gcp_cloud_storage:\n      bucket: b\n    azure_blob_storage:\n      storage_account: a\n      container: c\n",
			wantLint: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lints, err := linter.LintOutputYAML([]byte(base + tc.storage))
			require.NoError(t, err)
			var found []string
			for _, l := range lints {
				if strings.Contains(l.Error(), "at most one of") {
					found = append(found, l.Error())
				}
			}
			if tc.wantLint {
				assert.NotEmpty(t, found, "expected a storage lint, got: %v", lints)
			} else {
				assert.Empty(t, lints, "expected no lint")
			}
		})
	}
}

// TestParseStoragePropsRejectsTwoBlocks pins that the parser also rejects two
// storage blocks, so the config fails when lint does not run.
func TestParseStoragePropsRejectsTwoBlocks(t *testing.T) {
	conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    bucket: b
  gcp_cloud_storage:
    bucket: b
`, nil)
	require.NoError(t, err)

	_, err = parseStorageProps(conf)
	require.EqualError(t, err, "storage: specify at most one of aws_s3, gcp_cloud_storage or azure_blob_storage, got aws_s3, gcp_cloud_storage")
}

// TestRuntimeNoticeWiring pins the startup log notices wired in
// newIcebergOutputFromConfig (currently 0% covered): the max_in_flight ordering
// WARNING, the copy-on-write write-amplification INFO, and the
// cleanup_on_failure escape-hatch INFO. The pure helpers (mutating /
// cowAmplificationWarning) are unit-tested separately in cow_polish_test.go;
// this exercises the WIRING that decides whether each fires.
func TestRuntimeNoticeWiring(t *testing.T) {
	cases := []struct {
		name           string
		extra          string
		wantOrdering   bool
		wantAmp        bool
		wantCleanupOff bool
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
		{
			// The cleanup escape hatch left at its default: silent, so the
			// notice cannot become background noise for ordinary configs.
			name:           "cleanup_on_failure default silent",
			extra:          "commit:\n  cleanup_on_failure: true\n",
			wantCleanupOff: false,
		},
		{
			// Explicitly disabled: the notice fires, and only that one.
			name:           "cleanup_on_failure false informs",
			extra:          "commit:\n  cleanup_on_failure: false\n",
			wantCleanupOff: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := captureIcebergOutput(t, tc.extra)
			assert.Equal(t, tc.wantOrdering, orderingWarned(out),
				"ordering warning firing mismatch; captured log:\n%s", out)
			assert.Equal(t, tc.wantAmp, amplificationInformed(out),
				"amplification info firing mismatch; captured log:\n%s", out)
			assert.Equal(t, tc.wantCleanupOff, cleanupDisabledInformed(out),
				"cleanup_on_failure info firing mismatch; captured log:\n%s", out)
		})
	}
}

// TestParseStorageProps checks that each storage block gives the properties of
// its own backend, and that no storage block gives no properties. The bucket
// fields are deprecated and have no effect, so a config that sets them must
// parse and must give the same properties as one that does not.
func TestParseStorageProps(t *testing.T) {
	tests := []struct {
		name    string
		storage string
		want    map[string]string
	}{
		{
			name:    "no storage block",
			storage: ``,
			want:    map[string]string{},
		},
		{
			name: "aws_s3",
			storage: `
storage:
  aws_s3:
    region: us-east-1
`,
			want: map[string]string{
				"s3.region":                   "us-east-1",
				"s3.force-virtual-addressing": "true",
			},
		},
		{
			name: "aws_s3 with path style URLs",
			storage: `
storage:
  aws_s3:
    force_path_style_urls: true
`,
			want: map[string]string{
				"s3.force-virtual-addressing": "false",
			},
		},
		{
			name: "aws_s3 with deprecated bucket",
			storage: `
storage:
  aws_s3:
    bucket: my-bucket
`,
			want: map[string]string{
				"s3.force-virtual-addressing": "true",
			},
		},
		{
			name: "gcp_cloud_storage",
			storage: `
storage:
  gcp_cloud_storage:
    credentials_type: service_account
    credentials_json: '{"type":"service_account"}'
`,
			want: map[string]string{
				"gcs.credtype": "service_account",
				"gcs.jsonkey":  `{"type":"service_account"}`,
			},
		},
		{
			name: "gcp_cloud_storage with deprecated bucket",
			storage: `
storage:
  gcp_cloud_storage:
    bucket: my-bucket
`,
			want: map[string]string{},
		},
		{
			name: "azure_blob_storage with SAS token",
			storage: `
storage:
  azure_blob_storage:
    storage_account: myaccount
    container: mycontainer
    storage_sas_token: mytoken
`,
			want: map[string]string{
				"adls.sas-token.mycontainer": "mytoken",
			},
		},
		{
			name: "azure_blob_storage with access key",
			storage: `
storage:
  azure_blob_storage:
    storage_account: myaccount
    container: mycontainer
    storage_access_key: mykey
`,
			want: map[string]string{
				"adls.auth.shared-key.account.name": "myaccount",
				"adls.auth.shared-key.account.key":  "mykey",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
`+tt.storage, nil)
			require.NoError(t, err)

			props, err := parseStorageProps(conf)
			require.NoError(t, err)
			assert.Equal(t, tt.want, map[string]string(props))
		})
	}
}
