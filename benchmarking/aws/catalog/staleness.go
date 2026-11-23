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
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// ErrSHANotInRepo is returned by Git.ChangedPaths and Git.ChangelogMentions
// when the from SHA does not exist in the repository.
var ErrSHANotInRepo = errors.New("sha not in repo")

// Git answers the repository questions staleness checking needs.
type Git interface {
	// LatestRelease returns the highest vX.Y.Z tag.
	LatestRelease(ctx context.Context) (string, error)
	// MainRef returns the ref unreleased builds are compared against:
	// origin/main, or main when the repository has no such remote branch.
	MainRef(ctx context.Context) (string, error)
	// ComponentPaths returns the repo-relative directories under internal/impl
	// whose non-test Go files register the component. Empty means the
	// component is not registered in this repo (it is benthos-hosted).
	ComponentPaths(ctx context.Context, component string) ([]string, error)
	// BenthosVersion returns the github.com/redpanda-data/benthos/v4 version
	// required by the repo-root go.mod at ref.
	BenthosVersion(ctx context.Context, ref string) (string, error)
	// ChangedPaths returns the commits in fromSHA..toRef touching paths.
	ChangedPaths(ctx context.Context, fromSHA, toRef string, paths []string) ([]string, error)
	// ChangelogMentions returns CHANGELOG.md lines added in fromSHA..toRef
	// that mention the component.
	ChangelogMentions(ctx context.Context, fromSHA, toRef, component string) ([]string, error)
}

// ErrRepoRoot is returned by Git methods when the repository top level cannot
// be resolved from the configured path.
var ErrRepoRoot = errors.New("cannot resolve repo root")

type gitCLI struct {
	dir string // as given; may be any directory inside the repo

	once    sync.Once
	root    string // git top level of dir
	rootErr error
}

// NewGitCLI returns a Git backed by the git binary. repoDir may be the repo
// top level or any directory inside it; every command runs against the top
// level, so repo-relative pathspecs and go.mod lookups agree.
func NewGitCLI(repoDir string) Git { return &gitCLI{dir: repoDir} }

func (g *gitCLI) resolveRoot(ctx context.Context) (string, error) {
	g.once.Do(func() {
		cmd := exec.CommandContext(ctx, "git", "-C", g.dir, "rev-parse", "--show-toplevel")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			g.rootErr = fmt.Errorf("%w from %s: %w: %s", ErrRepoRoot, g.dir, err, strings.TrimSpace(stderr.String()))
			return
		}
		g.root = strings.TrimSpace(stdout.String())
	})
	return g.root, g.rootErr
}

func (g *gitCLI) run(ctx context.Context, args ...string) (string, error) {
	root, err := g.resolveRoot(ctx)
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func lines(s string) []string {
	var out []string
	for l := range strings.SplitSeq(s, "\n") {
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

func (g *gitCLI) MainRef(ctx context.Context) (string, error) {
	for _, ref := range []string{"origin/main", "main"} {
		_, err := g.run(ctx, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
		if err == nil {
			return ref, nil
		}
		if errors.Is(err, ErrRepoRoot) {
			return "", err
		}
	}
	return "", errors.New("neither origin/main nor main exists")
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

// ComponentPaths finds non-test files under internal/impl that register the
// component: a Register* call whose first argument is the quoted name,
// whitespace (including gofmt line breaks) allowed between them. git grep -F
// prefilters on the quoted name; the registration regex runs on the file
// contents.
func (g *gitCLI) ComponentPaths(ctx context.Context, component string) ([]string, error) {
	named, err := g.grepFiles(ctx, "-F", "-e", `"`+component+`"`, "--", "internal/impl", ":!*_test.go")
	if err != nil || len(named) == 0 {
		return nil, err
	}
	seen := map[string]bool{}
	var dirs []string
	for _, f := range named {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		root, err := g.resolveRoot(ctx)
		if err != nil {
			return nil, err
		}
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f)))
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", f, err)
		}
		if !registers(body, component) {
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

// registers reports whether src registers component by its literal name.
func registers(src []byte, component string) bool {
	re := regexp.MustCompile(`Register[A-Za-z]*\(\s*"` + regexp.QuoteMeta(component) + `"`)
	return re.Match(src)
}

const benthosModule = "github.com/redpanda-data/benthos/v4"

func (g *gitCLI) BenthosVersion(ctx context.Context, ref string) (string, error) {
	if err := g.checkSHA(ctx, ref); err != nil {
		return "", err
	}
	out, err := g.run(ctx, "show", ref+":go.mod")
	if err != nil {
		return "", err
	}
	return benthosRequire(out)
}

// benthosRequire returns the benthos version a go.mod requires.
func benthosRequire(gomod string) (string, error) {
	for l := range strings.SplitSeq(gomod, "\n") {
		f := strings.Fields(strings.TrimPrefix(strings.TrimSpace(l), "require "))
		if len(f) >= 2 && f[0] == benthosModule {
			return f[1], nil
		}
	}
	return "", fmt.Errorf("go.mod does not require %s", benthosModule)
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
	for l := range strings.SplitSeq(out, "\n") {
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
	State string `json:"state"` // "current" | "stale" | "unknown"
	// LatestRelease is the latest release tag, whatever the entry was
	// compared against.
	LatestRelease string `json:"latest_release"`
	// ComparedTo is the ref the entry was compared against: LatestRelease,
	// or the main ref for an unreleased entry.
	ComparedTo string   `json:"compared_to,omitempty"`
	Reasons    []string `json:"reasons"`
}

// CheckStaleness checks the entry's input, processors and output against code
// changes and CHANGELOG mentions between its build SHA and a later ref: the
// latest release for a released entry, or main (Git.MainRef) for an
// unreleased one, whose build is already past the latest tag.
func CheckStaleness(ctx context.Context, g Git, e Entry) (Staleness, error) {
	if !validSHA.MatchString(e.BuildSHA) {
		return Staleness{State: "unknown", Reasons: []string{"build sha unknown"}}, nil
	}
	latest, err := g.LatestRelease(ctx)
	if errors.Is(err, ErrRepoRoot) {
		return Staleness{State: "unknown", Reasons: []string{err.Error()}}, nil
	}
	if err != nil {
		return Staleness{}, err
	}
	toRef := latest
	if e.Unreleased() {
		if toRef, err = g.MainRef(ctx); err != nil {
			return Staleness{State: "unknown", LatestRelease: latest, Reasons: []string{err.Error()}}, nil
		}
	}
	unknown := func(reason string) Staleness {
		return Staleness{State: "unknown", LatestRelease: latest, ComparedTo: toRef, Reasons: []string{reason}}
	}
	s := Staleness{State: "current", LatestRelease: latest, ComparedTo: toRef, Reasons: []string{}}
	short := e.BuildSHA
	if len(short) > 7 {
		short = short[:7]
	}

	components := append([]string{e.Input}, e.Processors...)
	components = append(components, e.Output)
	seen := map[string]bool{}
	stale := false
	// Benthos versions at the build and at latest, read on first need.
	var benthosFrom, benthosTo string
	benthosRead := false
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
			// Not registered in this repo, so benthos hosts it: it changes
			// when the benthos dependency does.
			if !benthosRead {
				benthosRead = true
				benthosFrom, err = g.BenthosVersion(ctx, e.BuildSHA)
				if err == nil {
					benthosTo, err = g.BenthosVersion(ctx, toRef)
				}
				if errors.Is(err, ErrSHANotInRepo) {
					return unknown("build sha not in repo"), nil
				}
				if err != nil {
					reason := fmt.Sprintf("benthos version unknown (hosts %s): %v", c, err)
					if stale {
						// Earlier components already made the entry stale;
						// that stays true whatever benthos did.
						s.State = "stale"
						s.Reasons = append(s.Reasons, reason)
						return s, nil
					}
					return unknown(reason), nil
				}
			}
			if benthosFrom != benthosTo {
				stale = true
				s.Reasons = append(s.Reasons, fmt.Sprintf("benthos bumped %s → %s (hosts %s)", benthosFrom, benthosTo, c))
			} else {
				s.Reasons = append(s.Reasons, c+" is benthos-hosted; benthos unchanged")
			}
		} else {
			commits, err := g.ChangedPaths(ctx, e.BuildSHA, toRef, paths)
			if errors.Is(err, ErrSHANotInRepo) {
				return unknown("build sha not in repo"), nil
			}
			if err != nil {
				return Staleness{}, err
			}
			if len(commits) > 0 {
				stale = true
				s.Reasons = append(s.Reasons, fmt.Sprintf("%d commits to %s since %s", len(commits), strings.Join(paths, ", "), short))
			}
		}

		mentions, err := g.ChangelogMentions(ctx, e.BuildSHA, toRef, c)
		if errors.Is(err, ErrSHANotInRepo) {
			return unknown("build sha not in repo"), nil
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
