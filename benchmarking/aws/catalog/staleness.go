// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"
)

// ErrSHANotInRepo is returned by Git.ChangedPaths and Git.ChangelogMentions
// when the from SHA does not exist in the repository.
var ErrSHANotInRepo = errors.New("sha not in repo")

// Git answers the repository questions staleness checking needs.
type Git interface {
	// LatestRelease returns the highest vX.Y.Z tag.
	LatestRelease(ctx context.Context) (string, error)
	// ComponentPaths returns the repo-relative directories under internal/impl
	// whose non-test Go files register the component.
	ComponentPaths(ctx context.Context, component string) ([]string, error)
	// ChangedPaths returns the commits in fromSHA..toRef touching paths.
	ChangedPaths(ctx context.Context, fromSHA, toRef string, paths []string) ([]string, error)
	// ChangelogMentions returns CHANGELOG.md lines added in fromSHA..toRef
	// that mention the component.
	ChangelogMentions(ctx context.Context, fromSHA, toRef, component string) ([]string, error)
}

type gitCLI struct{ root string }

// NewGitCLI returns a Git backed by the git binary, run against repoRoot.
func NewGitCLI(repoRoot string) Git { return &gitCLI{root: repoRoot} }

func (g *gitCLI) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", g.root}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// validSHA guards what reaches git: hex only, abbreviated or full.
var validSHA = regexp.MustCompile(`^[0-9a-fA-F]{7,40}$`)

var releaseTag = regexp.MustCompile(`^v4\.\d+\.\d+$`)

func (g *gitCLI) LatestRelease(ctx context.Context) (string, error) {
	out, err := g.run(ctx, "tag", "--list", "v4.*", "--sort=-v:refname")
	if err != nil {
		return "", err
	}
	for _, t := range lines(out) {
		if releaseTag.MatchString(t) {
			return t, nil
		}
	}
	return "", errors.New("no v4.x.y release tag found")
}

// grepFiles runs git grep -l and treats exit status 1 (no matches) as empty.
func (g *gitCLI) grepFiles(ctx context.Context, args ...string) ([]string, error) {
	out, err := g.run(ctx, append([]string{"grep", "-l"}, args...)...)
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && ee.ExitCode() == 1 {
			return nil, nil
		}
		return nil, err
	}
	return lines(out), nil
}

// ComponentPaths finds non-test files under internal/impl containing the
// quoted component name and a Register call. The name is matched anywhere in
// the file because gofmt often puts it on the line after the Register call.
func (g *gitCLI) ComponentPaths(ctx context.Context, component string) ([]string, error) {
	named, err := g.grepFiles(ctx, "-F", "-e", `"`+component+`"`, "--", "internal/impl", ":!*_test.go")
	if err != nil || len(named) == 0 {
		return nil, err
	}
	files, err := g.grepFiles(ctx, append([]string{"-E", `Register[A-Za-z]*\(`, "--"}, named...)...)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var dirs []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if d := path.Dir(f); !seen[d] {
			seen[d] = true
			dirs = append(dirs, d)
		}
	}
	sort.Strings(dirs)
	return dirs, nil
}

func (g *gitCLI) checkSHA(ctx context.Context, sha string) error {
	if _, err := g.run(ctx, "cat-file", "-e", sha+"^{commit}"); err != nil {
		return fmt.Errorf("%w: %s", ErrSHANotInRepo, sha)
	}
	return nil
}

func (g *gitCLI) ChangedPaths(ctx context.Context, fromSHA, toRef string, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	if err := g.checkSHA(ctx, fromSHA); err != nil {
		return nil, err
	}
	args := append([]string{"log", "--format=%h", fromSHA + ".." + toRef, "--"}, paths...)
	out, err := g.run(ctx, args...)
	if err != nil {
		return nil, err
	}
	return lines(out), nil
}

func (g *gitCLI) ChangelogMentions(ctx context.Context, fromSHA, toRef, component string) ([]string, error) {
	if err := g.checkSHA(ctx, fromSHA); err != nil {
		return nil, err
	}
	out, err := g.run(ctx, "diff", "--unified=0", fromSHA+".."+toRef, "--", "CHANGELOG.md")
	if err != nil {
		return nil, err
	}
	var hits []string
	for _, l := range strings.Split(out, "\n") {
		if !strings.HasPrefix(l, "+") || strings.HasPrefix(l, "+++") {
			continue
		}
		l = strings.TrimSpace(strings.TrimPrefix(l, "+"))
		if strings.HasPrefix(l, "- "+component+":") || strings.Contains(l, "`"+component+"`") {
			hits = append(hits, l)
		}
	}
	return hits, nil
}

// Staleness is whether a stored result is still current.
type Staleness struct {
	State         string   `json:"state"` // "current" | "stale" | "unknown"
	LatestRelease string   `json:"latest_release"`
	Reasons       []string `json:"reasons"`
}

// CheckStaleness checks the entry's input, processors and output against code
// changes and CHANGELOG mentions between its build SHA and the latest release.
func CheckStaleness(ctx context.Context, g Git, e Entry) (Staleness, error) {
	if !validSHA.MatchString(e.BuildSHA) {
		return Staleness{State: "unknown", Reasons: []string{"build sha unknown"}}, nil
	}
	latest, err := g.LatestRelease(ctx)
	if err != nil {
		return Staleness{}, err
	}
	s := Staleness{State: "current", LatestRelease: latest, Reasons: []string{}}
	short := e.BuildSHA
	if len(short) > 7 {
		short = short[:7]
	}

	components := append([]string{e.Input}, e.Processors...)
	components = append(components, e.Output)
	seen := map[string]bool{}
	stale := false
	for _, c := range components {
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true

		paths, err := g.ComponentPaths(ctx, c)
		if err != nil {
			return Staleness{}, err
		}
		if len(paths) == 0 {
			s.Reasons = append(s.Reasons, "no source path for "+c)
		} else {
			commits, err := g.ChangedPaths(ctx, e.BuildSHA, latest, paths)
			if errors.Is(err, ErrSHANotInRepo) {
				return Staleness{State: "unknown", LatestRelease: latest, Reasons: []string{"build sha not in repo"}}, nil
			}
			if err != nil {
				return Staleness{}, err
			}
			if len(commits) > 0 {
				stale = true
				s.Reasons = append(s.Reasons, fmt.Sprintf("%d commits to %s since %s", len(commits), strings.Join(paths, ", "), short))
			}
		}

		mentions, err := g.ChangelogMentions(ctx, e.BuildSHA, latest, c)
		if errors.Is(err, ErrSHANotInRepo) {
			return Staleness{State: "unknown", LatestRelease: latest, Reasons: []string{"build sha not in repo"}}, nil
		}
		if err != nil {
			return Staleness{}, err
		}
		for _, m := range mentions {
			stale = true
			s.Reasons = append(s.Reasons, "CHANGELOG: "+m)
		}
	}
	if stale {
		s.State = "stale"
	}
	return s, nil
}
