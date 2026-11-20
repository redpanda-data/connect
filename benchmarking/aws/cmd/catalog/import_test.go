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

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false"}, args...)...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

// tempRepo creates a git repo whose HEAD is tagged v4.1.0 and returns its
// path and HEAD sha.
func tempRepo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644))
	git(t, dir, "add", "f")
	git(t, dir, "commit", "-q", "-m", "init")
	git(t, dir, "tag", "v4.1.0")
	return dir, git(t, dir, "rev-parse", "HEAD")
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

	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+store, "--results-root", root,
		"--repo", repo, "--scenarios-root", filepath.Join("testdata", "scenarios"))
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
	require.Equal(t, []string{"benchmark"}, e.Processors)
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
	repo, _ := tempRepo(t)
	// The fixture sha is not a commit in this repo, so git describe fails.
	root := copyFixtures(t, "0123456789abcdef0123456789abcdef01234567")
	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+t.TempDir(), "--results-root", root,
		"--repo", repo, "--scenarios-root", filepath.Join("testdata", "scenarios"))
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

func TestImportEmptyRoot(t *testing.T) {
	code, stdout, stderr := runCLI(t, "import", "--store", "dir:"+t.TempDir(), "--results-root", t.TempDir(),
		"--repo", t.TempDir(), "--scenarios-root", t.TempDir())
	require.Equal(t, 0, code, stderr)
	require.JSONEq(t, `{"imported":[],"skipped":[]}`, stdout)
}
