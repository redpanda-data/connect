// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeGit struct {
	release   string
	paths     map[string][]string
	changed   map[string][]string // keyed by joined paths
	changelog map[string][]string
	changeErr error
}

func (f *fakeGit) LatestRelease(context.Context) (string, error) { return f.release, nil }

func (f *fakeGit) ComponentPaths(_ context.Context, c string) ([]string, error) {
	return f.paths[c], nil
}

func (f *fakeGit) ChangedPaths(_ context.Context, _, _ string, paths []string) ([]string, error) {
	if f.changeErr != nil {
		return nil, f.changeErr
	}
	var out []string
	for _, p := range paths {
		out = append(out, f.changed[p]...)
	}
	return out, nil
}

func (f *fakeGit) ChangelogMentions(_ context.Context, _, _, c string) ([]string, error) {
	return f.changelog[c], nil
}

func stalenessEntry() Entry {
	return Entry{
		Input:      "postgres_cdc",
		Processors: []string{"mapping"},
		Output:     "snowflake_streaming",
		BuildSHA:   "abcdef1234567",
	}
}

func stalenessFake() *fakeGit {
	return &fakeGit{
		release: "v4.99.0",
		paths: map[string][]string{
			"postgres_cdc":        {"internal/impl/postgresql"},
			"mapping":             {"internal/impl/pure"},
			"snowflake_streaming": {"internal/impl/snowflake"},
		},
	}
}

func TestStalenessCurrent(t *testing.T) {
	s, err := CheckStaleness(t.Context(), stalenessFake(), stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "current", s.State)
	require.Equal(t, "v4.99.0", s.LatestRelease)
	require.Empty(t, s.Reasons)
}

func TestStalenessStaleViaPath(t *testing.T) {
	g := stalenessFake()
	g.changed = map[string][]string{"internal/impl/postgresql": {"aaa1111", "bbb2222", "ccc3333"}}
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "stale", s.State)
	require.Equal(t, []string{"3 commits to internal/impl/postgresql since abcdef1"}, s.Reasons)
}

func TestStalenessStaleViaChangelog(t *testing.T) {
	g := stalenessFake()
	g.changelog = map[string][]string{"postgres_cdc": {"- postgres_cdc: fixed a thing"}}
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "stale", s.State)
	require.Equal(t, []string{"CHANGELOG: - postgres_cdc: fixed a thing"}, s.Reasons)
}

func TestStalenessUnknownSHA(t *testing.T) {
	for _, sha := range []string{"", "unknown"} {
		e := stalenessEntry()
		e.BuildSHA = sha
		s, err := CheckStaleness(t.Context(), stalenessFake(), e)
		require.NoError(t, err)
		require.Equal(t, "unknown", s.State)
		require.Equal(t, []string{"build sha unknown"}, s.Reasons)
	}
}

func TestStalenessSHANotInRepo(t *testing.T) {
	g := stalenessFake()
	g.changeErr = ErrSHANotInRepo
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "unknown", s.State)
	require.Equal(t, []string{"build sha not in repo"}, s.Reasons)
}

func TestStalenessOtherGitErrorPropagates(t *testing.T) {
	g := stalenessFake()
	g.changeErr = errors.New("boom")
	_, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.ErrorContains(t, err, "boom")
}

func TestStalenessUnresolvableComponent(t *testing.T) {
	g := stalenessFake()
	delete(g.paths, "mapping")
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "current", s.State)
	require.Equal(t, []string{"no source path for mapping"}, s.Reasons)
}

func TestStalenessGitCLIAgainstRepo(t *testing.T) {
	g := NewGitCLI("../../..")
	ctx := t.Context()
	if _, err := g.LatestRelease(ctx); err != nil {
		t.Skipf("git unavailable or not a repo: %v", err)
	}
	tag, err := g.LatestRelease(ctx)
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`^v4\.\d+\.\d+$`), tag)

	paths, err := g.ComponentPaths(ctx, "postgres_cdc")
	require.NoError(t, err)
	require.Contains(t, paths, "internal/impl/postgresql")
	for _, p := range paths {
		require.NotContains(t, p, "_test.go")
	}

	none, err := g.ComponentPaths(ctx, "no_such_component_xyz")
	require.NoError(t, err)
	require.Empty(t, none)

	_, err = g.ChangedPaths(ctx, "0000000000000000000000000000000000000000", tag, paths)
	require.ErrorIs(t, err, ErrSHANotInRepo)
}
