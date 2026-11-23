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
