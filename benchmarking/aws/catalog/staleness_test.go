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
	release    string
	paths      map[string][]string
	changed    map[string][]string // keyed by joined paths
	changelog  map[string][]string
	changeErr  error
	benthos    map[string]string // keyed by ref
	benthosErr error
}

func (f *fakeGit) BenthosVersion(_ context.Context, ref string) (string, error) {
	if f.benthosErr != nil {
		return "", f.benthosErr
	}
	return f.benthos[ref], nil
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

// stalenessFake resolves the entry's input and output to source paths;
// mapping has none, so it is benthos-hosted.
func stalenessFake() *fakeGit {
	return &fakeGit{
		release: "v4.99.0",
		paths: map[string][]string{
			"postgres_cdc":        {"internal/impl/postgresql"},
			"snowflake_streaming": {"internal/impl/snowflake"},
		},
		benthos: map[string]string{"abcdef1234567": "v4.80.0", "v4.99.0": "v4.80.0"},
	}
}

func TestStalenessCurrent(t *testing.T) {
	s, err := CheckStaleness(t.Context(), stalenessFake(), stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "current", s.State)
	require.Equal(t, "v4.99.0", s.LatestRelease)
	require.Equal(t, []string{"mapping is benthos-hosted; benthos unchanged"}, s.Reasons)
}

func TestStalenessStaleViaPath(t *testing.T) {
	g := stalenessFake()
	g.changed = map[string][]string{"internal/impl/postgresql": {"aaa1111", "bbb2222", "ccc3333"}}
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "stale", s.State)
	require.Equal(t, []string{
		"3 commits to internal/impl/postgresql since abcdef1",
		"mapping is benthos-hosted; benthos unchanged",
	}, s.Reasons)
}

func TestStalenessStaleViaChangelog(t *testing.T) {
	g := stalenessFake()
	g.changelog = map[string][]string{"postgres_cdc": {"- postgres_cdc: fixed a thing"}}
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "stale", s.State)
	require.Equal(t, []string{
		"CHANGELOG: - postgres_cdc: fixed a thing",
		"mapping is benthos-hosted; benthos unchanged",
	}, s.Reasons)
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

func TestStalenessBenthosBumped(t *testing.T) {
	g := stalenessFake()
	g.benthos["v4.99.0"] = "v4.85.0"
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "stale", s.State)
	require.Equal(t, []string{"benthos bumped v4.80.0 → v4.85.0 (hosts mapping)"}, s.Reasons)
}

func TestStalenessBenthosUnreadable(t *testing.T) {
	g := stalenessFake()
	g.benthosErr = errors.New("no go.mod")
	s, err := CheckStaleness(t.Context(), g, stalenessEntry())
	require.NoError(t, err)
	require.Equal(t, "unknown", s.State)
	require.Len(t, s.Reasons, 1)
	require.Contains(t, s.Reasons[0], "no go.mod")
	require.Contains(t, s.Reasons[0], "mapping")
}

func TestStalenessBenthosNotCheckedWhenAllComponentsResolve(t *testing.T) {
	g := stalenessFake()
	g.benthosErr = errors.New("must not be called")
	e := stalenessEntry()
	e.Processors = nil
	s, err := CheckStaleness(t.Context(), g, e)
	require.NoError(t, err)
	require.Equal(t, "current", s.State)
	require.Empty(t, s.Reasons)
}

func TestRegistersComponent(t *testing.T) {
	for name, tc := range map[string]struct {
		src  string
		want bool
	}{
		"same line":     {`service.MustRegisterBatchInput("postgres_cdc", spec, ctor)`, true},
		"gofmt wrapped": {"service.MustRegisterBatchOutput(\n\t\t\"postgres_cdc\",\n\t\tspec)", true},
		"other name":    {`service.MustRegisterBatchInput("postgres_cdc_legacy", spec, ctor)`, false},
		"name not registered": {`service.MustRegisterBatchInput("x", spec, ctor)
var f = "postgres_cdc"`, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, registers([]byte(tc.src), "postgres_cdc"))
		})
	}
	// Regex metacharacters in the name are literal.
	require.False(t, registers([]byte(`RegisterInput("aXb", s)`), "a.b"))
}

func TestBenthosRequire(t *testing.T) {
	v, err := benthosRequire("module x\n\nrequire (\n\tgithub.com/foo/bar v1.0.0\n\tgithub.com/redpanda-data/benthos/v4 v4.85.0 // indirect\n)\n")
	require.NoError(t, err)
	require.Equal(t, "v4.85.0", v)

	v, err = benthosRequire("module x\nrequire github.com/redpanda-data/benthos/v4 v4.1.0\n")
	require.NoError(t, err)
	require.Equal(t, "v4.1.0", v)

	_, err = benthosRequire("module x\nrequire github.com/redpanda-data/benthos/v4/extra v1.0.0\n")
	require.Error(t, err)
}

func TestStalenessInvalidSHA(t *testing.T) {
	for _, sha := range []string{"--output=x", "xyz1234", "abc12", "abcdef1234567890abcdef1234567890abcdef123", "HEAD"} {
		e := stalenessEntry()
		e.BuildSHA = sha
		s, err := CheckStaleness(t.Context(), stalenessFake(), e)
		require.NoError(t, err, sha)
		require.Equal(t, "unknown", s.State, sha)
		require.Equal(t, []string{"build sha unknown"}, s.Reasons, sha)
	}
}

func TestStalenessChangelogMentionsAgainstRepo(t *testing.T) {
	g := NewGitCLI("../../..")
	ctx := t.Context()
	const from, to = "v4.118.0", "v4.119.0"
	if _, err := g.ChangedPaths(ctx, from, to, []string{"CHANGELOG.md"}); err != nil {
		t.Skipf("tags %s..%s unavailable: %v", from, to, err)
	}

	got, err := g.ChangelogMentions(ctx, from, to, "oracledb_cdc")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Contains(t, got[0], "- oracledb_cdc: Concurrent pipeline starts")

	got, err = g.ChangelogMentions(ctx, from, to, "snowflake_streaming")
	require.NoError(t, err)
	require.Empty(t, got)
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
	require.Equal(t, []string{"internal/impl/postgresql"}, paths)

	// Registered with the name on a separate line from the Register call.
	snow, err := g.ComponentPaths(ctx, "snowflake_streaming")
	require.NoError(t, err)
	require.Equal(t, []string{"internal/impl/snowflake"}, snow)

	// Hosted by benthos: the name appears in this repo but is not registered here.
	mapping, err := g.ComponentPaths(ctx, "mapping")
	require.NoError(t, err)
	require.Empty(t, mapping)

	v, err := g.BenthosVersion(ctx, tag)
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`^v4\.\d+\.\d+`), v)
	_, err = g.BenthosVersion(ctx, "0000000000000000000000000000000000000000")
	require.ErrorIs(t, err, ErrSHANotInRepo)

	none, err := g.ComponentPaths(ctx, "no_such_component_xyz")
	require.NoError(t, err)
	require.Empty(t, none)

	_, err = g.ChangedPaths(ctx, "0000000000000000000000000000000000000000", tag, paths)
	require.ErrorIs(t, err, ErrSHANotInRepo)
}
