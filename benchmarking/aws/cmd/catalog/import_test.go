// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/benchmarking/aws/catalog"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false"}, args...)...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

// scenariosPath is where the importer reads scenarios by default.
const scenariosPath = "benchmarking/aws/scenarios"

// untaggedRepo creates a git repo holding testdata/scenarios at
// scenariosPath and returns its path and HEAD sha.
func untaggedRepo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q")
	src := filepath.Join("testdata", "scenarios")
	require.NoError(t, filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		out := filepath.Join(dir, scenariosPath, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return os.WriteFile(out, b, 0o644)
	}))
	git(t, dir, "add", ".")
	git(t, dir, "commit", "-q", "-m", "init")
	return dir, git(t, dir, "rev-parse", "HEAD")
}

// tempRepo is untaggedRepo with HEAD tagged v4.1.0.
func tempRepo(t *testing.T) (string, string) {
	t.Helper()
	dir, sha := untaggedRepo(t)
	git(t, dir, "tag", "v4.1.0")
	return dir, sha
}

// copyFixtures copies testdata/results into a temp dir, substituting the
// real repo sha for @SHA@.
func copyFixtures(t *testing.T, sha string) string {
	t.Helper()
	dst := t.TempDir()
	src := filepath.Join("testdata", "results")
	require.NoError(t, filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		out := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return os.WriteFile(out, []byte(strings.ReplaceAll(string(b), "@SHA@", sha)), 0o644)
	}))
	return dst
}

type importOut struct {
	Imported []string `json:"imported"`
	Skipped  []struct {
		Path   string `json:"path"`
		Reason string `json:"reason"`
	} `json:"skipped"`
}

func TestImportSkipsAndImports(t *testing.T) {
	repo, sha := tempRepo(t)
	root := copyFixtures(t, sha)
	store := t.TempDir()

	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+store, "--results-root", root, "--repo", repo)
	require.Equal(t, 0, code, stderr)

	var got importOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Equal(t, []string{"postgres-orders-cdc-2026-06-01T10-00-00Z"}, got.Imported)

	reasons := map[string]string{}
	for _, s := range got.Skipped {
		reasons[filepath.Base(s.Path)] = s.Reason
	}
	require.Equal(t, map[string]string{
		"2026-06-02T10-00-00Z.json": "build sha unknown",
		"2026-06-03T10-00-00Z.json": "runner instance type missing",
		"2026-06-04T10-00-00Z.json": "scenario yaml missing",
		"2026-06-05T10-00-00Z.json": "soak result",
		"2026-06-06T10-00-00Z.json": "sink direction not supported",
	}, reasons)

	entries, err := catalog.NewDirStore(store).List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	e := entries[0]
	require.Equal(t, "v4.1.0", e.ConnectVersion)
	require.Equal(t, sha, e.BuildSHA)
	require.Equal(t, "postgres_cdc", e.Input)
	require.Equal(t, "redpanda", e.Output)
	require.Empty(t, e.Processors)
	require.True(t, e.SinkMeasured)
	require.Nil(t, e.AWSCostUSD)
	require.Equal(t, "cdc", e.Workload.Mode)
	require.Equal(t, 1200, e.Workload.MessageSizeBytes)
	require.Equal(t, 150000, e.Workload.OfferedRatePerSec)
	require.False(t, e.MemoryLimited)
	require.Equal(t, "us-east-2", e.Infra.Region)
	require.Len(t, e.Points, 2)
	require.Equal(t, uint64(1500), e.Points[0].PeakRSSBytes)
	require.Equal(t, 2026, e.RecordedAt.Year())
}

func TestImportConnectVersionUnknown(t *testing.T) {
	// No v4.* tag reaches the sha, so git describe fails.
	repo, sha := untaggedRepo(t)
	root := copyFixtures(t, sha)
	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+t.TempDir(), "--results-root", root, "--repo", repo)
	require.Equal(t, 0, code, stderr)
	var got importOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got))
	require.Empty(t, got.Imported)
	var found bool
	for _, s := range got.Skipped {
		if filepath.Base(s.Path) == "2026-06-01T10-00-00Z.json" {
			require.Equal(t, "connect version unknown", s.Reason)
			found = true
		}
	}
	require.True(t, found)
}

func TestImportReadsScenarioAtResultSHA(t *testing.T) {
	repo, sha := tempRepo(t)
	// After the measured commit, the scenario's offered rate changes and a
	// scenario that did not exist at the measured commit appears.
	cdc := filepath.Join(repo, scenariosPath, "postgres", "orders-cdc.yaml")
	b, err := os.ReadFile(cdc)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cdc, []byte(strings.Replace(string(b), "150000", "999", 1)), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, scenariosPath, "postgres", "orders-missing.yaml"), b, 0o644))
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "later")

	root := copyFixtures(t, sha)
	store := t.TempDir()
	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+store, "--results-root", root, "--repo", repo)
	require.Equal(t, 0, code, stderr)
	var got importOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Equal(t, []string{"postgres-orders-cdc-2026-06-01T10-00-00Z"}, got.Imported)
	for _, s := range got.Skipped {
		if filepath.Base(s.Path) == "2026-06-04T10-00-00Z.json" {
			require.Equal(t, "scenario yaml missing", s.Reason)
		}
	}
	entries, err := catalog.NewDirStore(store).List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, 150000, entries[0].Workload.OfferedRatePerSec)
}

func TestImportMalformedFileIsSkipped(t *testing.T) {
	repo, sha := tempRepo(t)
	root := copyFixtures(t, sha)
	bad := filepath.Join(root, "postgres", "orders-cdc", "2026-06-00T10-00-00Z.json")
	require.NoError(t, os.WriteFile(bad, []byte(`{"scenario":`), 0o644))

	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+t.TempDir(), "--results-root", root, "--repo", repo)
	require.Equal(t, 0, code, stderr)
	var got importOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Equal(t, []string{"postgres-orders-cdc-2026-06-01T10-00-00Z"}, got.Imported)
	require.Equal(t, bad, got.Skipped[0].Path)
	require.Contains(t, got.Skipped[0].Reason, "decoding result")
}

func TestEntryFromResultFileSkipReasons(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, "postgres", "orders-cdc", name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
		return p
	}
	describe := func(string) (string, error) { return "v4.1.0", nil }
	var gitCalls int
	scenarioAt := func(_, _ string) ([]byte, error) {
		gitCalls++
		return []byte("pipeline:\n  input:\n    postgres_cdc: {}\n"), nil
	}

	// A non-hex sha never reaches git.
	f := write("a.json", `{"scenario":"postgres/orders-cdc","git_sha":"--output=x"}`)
	_, reason := entryFromResultFile(f, dir, scenarioAt, describe)
	require.Equal(t, "build sha unknown", reason)
	require.Zero(t, gitCalls)

	// An EntryFromRun error other than the known rejections is a skip too.
	f = write("b.json", `{"scenario":"postgres/orders-cdc","git_sha":"abcdef1","infra":{"runner_instance_type":"c8g.4xlarge"}}`)
	_, reason = entryFromResultFile(f, dir, scenarioAt, describe)
	require.Contains(t, reason, "invalid entry")

	scenarioAt = func(_, _ string) ([]byte, error) { return []byte("pipeline: [unclosed"), nil }
	_, reason = entryFromResultFile(f, dir, scenarioAt, describe)
	require.Contains(t, reason, "decoding scenario")
}

func TestImportScenariosRootMustBeRelative(t *testing.T) {
	for _, root := range []string{"/abs/path", "../outside", ""} {
		code, _, _ := runCLI(t, "import", "--store", "dir:"+t.TempDir(), "--results-root", t.TempDir(), "--scenarios-root", root)
		require.Equal(t, 2, code, root)
	}
}

func TestImportEmptyRoot(t *testing.T) {
	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+t.TempDir(), "--results-root", t.TempDir(),
		"--repo", t.TempDir())
	require.Equal(t, 0, code, stderr)
	require.JSONEq(t, `{"imported":[],"skipped":[]}`, stdout)
}

// writeResult writes a minimal postgres/orders-cdc result under root and
// sessionID may be empty for a pre-session-ID result.
func writeResult(t *testing.T, root, scenario, stamp, sha, sessionID string) {
	t.Helper()
	body := map[string]any{
		"scenario": scenario, "git_sha": sha,
		"started_at": "2026-07-01T10:00:00Z", "finished_at": "2026-07-01T11:00:00Z",
		"infra":   map[string]any{"runner_instance_type": "c8g.4xlarge", "region": "us-east-2"},
		"dataset": map[string]any{"row_size_bytes": 1200},
		"points": []any{map[string]any{
			"vcpu": 1, "engine": "connect",
			"summary": map[string]any{"median_mb_s": 18, "median_msg_s": 15000},
		}},
	}
	if sessionID != "" {
		body["session_id"] = sessionID
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	p := filepath.Join(root, filepath.FromSlash(scenario), stamp+".json")
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, raw, 0o644))
}

func TestImportEntryIDs(t *testing.T) {
	const (
		stamp       = "2026-07-01T10-00-00Z"
		pathID      = "postgres-orders-cdc-" + stamp
		sessionID   = "bench-abc123"
		runnerTitle = "runner wrote this"
	)
	tests := []struct {
		name         string
		sessionID    string
		seed         bool // runner already catalogued the run
		runTwice     bool
		wantImported []string
		wantSkipped  string
	}{
		{name: "session id is the entry id", sessionID: sessionID, wantImported: []string{sessionID}},
		{name: "no session id keeps the path id", wantImported: []string{pathID}},
		{name: "second import adds nothing", sessionID: sessionID, runTwice: true, wantSkipped: "already in catalogue"},
		{name: "runner entry is not overwritten", sessionID: sessionID, seed: true, wantSkipped: "already in catalogue"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, sha := tempRepo(t)
			root := t.TempDir()
			writeResult(t, root, "postgres/orders-cdc", stamp, sha, tc.sessionID)
			store := t.TempDir()
			ctx := context.Background()

			if tc.seed {
				// An entry as the runner would have stored it under the session ID.
				seedRoot := t.TempDir()
				writeResult(t, seedRoot, "postgres/orders-cdc", "2026-06-30T10-00-00Z", sha, tc.sessionID)
				code, _, stderr := runCLI(t, "import", "--store", "dir:"+store, "--results-root", seedRoot, "--repo", repo)
				require.Equal(t, 0, code, stderr)
				st := catalog.NewDirStore(store)
				seeded, err := st.List(ctx, "")
				require.NoError(t, err)
				require.Len(t, seeded, 1)
				seeded[0].ConnectVersion = runnerTitle
				require.NoError(t, st.Put(ctx, seeded[0]))
			}
			runOnce := func() importOut {
				code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+store, "--results-root", root, "--repo", repo)
				require.Equal(t, 0, code, stderr)
				var got importOut
				require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
				return got
			}
			got := runOnce()
			if tc.runTwice {
				require.Equal(t, []string{tc.sessionID}, got.Imported)
				got = runOnce()
			}

			entries, err := catalog.NewDirStore(store).List(ctx, "")
			require.NoError(t, err)
			require.Len(t, entries, 1)
			if tc.wantSkipped != "" {
				require.Empty(t, got.Imported)
				require.Len(t, got.Skipped, 1)
				require.Equal(t, tc.wantSkipped, got.Skipped[0].Reason)
				require.Equal(t, sessionID, entries[0].ID)
				if tc.seed {
					require.Equal(t, runnerTitle, entries[0].ConnectVersion)
				}
				return
			}
			require.Equal(t, tc.wantImported, got.Imported)
			require.Equal(t, tc.wantImported[0], entries[0].ID)
		})
	}
}

func TestImportSkipsE2EDirection(t *testing.T) {
	repo, _ := tempRepo(t)
	scen := filepath.Join(repo, scenariosPath, "postgres", "orders-e2e.yaml")
	require.NoError(t, os.WriteFile(scen, []byte("name: postgres-orders-e2e\nconnector: postgres_cdc\ndirection: e2e\npipeline:\n  input:\n    postgres_cdc: {}\n"), 0o644))
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "e2e")
	sha := git(t, repo, "rev-parse", "HEAD")

	root := t.TempDir()
	writeResult(t, root, "postgres/orders-e2e", "2026-07-02T10-00-00Z", sha, "bench-e2e1")
	store := t.TempDir()
	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+store, "--results-root", root, "--repo", repo)
	require.Equal(t, 0, code, stderr)
	var got importOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &got), stdout)
	require.Empty(t, got.Imported)
	require.Len(t, got.Skipped, 1)
	require.Equal(t, "e2e runs are catalogued by the run itself", got.Skipped[0].Reason)
	entries, err := catalog.NewDirStore(store).List(context.Background(), "")
	require.NoError(t, err)
	require.Empty(t, entries)
}
