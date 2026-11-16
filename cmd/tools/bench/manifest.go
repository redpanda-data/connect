// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	manifestFile = "bench.yaml"
	// suitesRoot is where suites are searched, relative to the repository root.
	suitesRoot = "internal/impl"

	taskUp    = "bench:up"
	taskLoad  = "bench:load"
	taskRun   = "bench:run"
	taskReset = "bench:reset"
	taskDown  = "bench:down"

	defaultMaxDuration = 10 * time.Minute
)

// requiredTasks is the task interface every suite exposes. A suite with
// nothing to do for one of them still declares it, as a no-op, so that a
// caller never has to read the Taskfile to know what exists.
var requiredTasks = []string{taskUp, taskLoad, taskRun, taskReset, taskDown}

var (
	taskfileNames = []string{"Taskfile.yaml", "Taskfile.yml"}
	nameRe        = regexp.MustCompile(`^[a-z0-9]+(?:[_-][a-z0-9]+)*$`)
	varRe         = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	kinds         = []string{"input", "output", "processor", "cache", "pipeline"}
)

// Manifest is the bench.yaml of a suite: what it measures, what it needs, and
// which scenarios it runs.
type Manifest struct {
	// Name identifies the suite on the command line. Unique in the repository.
	Name string `yaml:"name" json:"name,omitempty"`
	// Component is the connector under test, by its config name.
	Component string `yaml:"component" json:"component,omitempty"`
	// Kind is the component type: input, output, processor, cache, or
	// pipeline for a suite that measures several together.
	Kind    string `yaml:"kind" json:"kind,omitempty"`
	Summary string `yaml:"summary" json:"summary,omitempty"`
	// Results is the repository path of the file that records past runs.
	Results   string     `yaml:"results" json:"results,omitempty"`
	Requires  Requires   `yaml:"requires" json:"requires"`
	Scenarios []Scenario `yaml:"scenarios" json:"scenarios,omitempty"`
}

// Requires declares what must be in place before the suite can run.
type Requires struct {
	// Tools are the executables the tasks call. A "docker" entry also
	// requires a reachable Docker daemon.
	Tools []string `yaml:"tools" json:"tools,omitempty"`
	// Env are the environment variables that must be set, such as the DSN of
	// a system that cannot run in Docker.
	Env []string `yaml:"env" json:"env,omitempty"`
	// Notes covers what a check cannot: licences, accounts, disk space.
	Notes string `yaml:"notes" json:"notes,omitempty"`
}

// Scenario is one measurement of a suite.
type Scenario struct {
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description,omitempty"`
	// Task is the task that runs the scenario. Defaults to bench:run.
	Task string `yaml:"task" json:"task,omitempty"`
	// Vars are passed to every task of the run as Taskfile variables.
	Vars map[string]string `yaml:"vars" json:"vars,omitempty"`
	// MaxDuration bounds the run phase. Defaults to ten minutes.
	MaxDuration string `yaml:"max_duration" json:"max_duration,omitempty"`
}

func (s Scenario) runTask() string {
	if s.Task != "" {
		return s.Task
	}
	return taskRun
}

func (s Scenario) maxDuration() time.Duration {
	if d, err := time.ParseDuration(s.MaxDuration); err == nil && d > 0 {
		return d
	}
	return defaultMaxDuration
}

// Suite is a benchmark directory. It is conformant when it declares a valid
// manifest and the task interface; the other ones are listed with what they
// lack so they stay discoverable until they are migrated.
type Suite struct {
	// Dir is relative to the repository root, with forward slashes.
	Dir        string   `json:"dir"`
	Conformant bool     `json:"conformant"`
	Problems   []string `json:"problems,omitempty"`
	// Tasks are the tasks of the suite's Taskfile.
	Tasks []string `json:"tasks,omitempty"`
	Manifest

	// declared is set when the directory has a manifest, valid or not.
	declared bool
}

func (s *Suite) scenario(name string) (Scenario, error) {
	if name == "" {
		return s.Scenarios[0], nil
	}
	for _, sc := range s.Scenarios {
		if sc.Name == name {
			return sc, nil
		}
	}
	return Scenario{}, fmt.Errorf("suite %s has no scenario %q", s.Name, name)
}

// repoRoot returns the root of the checkout the tool runs in.
func repoRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("finding the repository root: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// discover returns every benchmark suite under root, sorted by directory: the
// directories with a manifest, and the ones under a bench directory that only
// have a Taskfile.
func discover(root string) ([]*Suite, error) {
	var suites []*Suite
	err := filepath.WalkDir(filepath.Join(root, suitesRoot), func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		_, statErr := os.Stat(filepath.Join(path, manifestFile))
		hasManifest := statErr == nil
		taskfile := findTaskfile(path)
		legacy := taskfile != "" && slices.Contains(strings.Split(rel, "/"), "bench")
		if !hasManifest && !legacy {
			return nil
		}

		s := &Suite{Dir: rel, declared: hasManifest}
		if taskfile != "" {
			tasks, err := taskNames(taskfile)
			if err != nil {
				s.Problems = append(s.Problems, err.Error())
			}
			s.Tasks = tasks
		}
		if hasManifest {
			m, err := loadManifest(filepath.Join(path, manifestFile))
			if err != nil {
				s.Problems = append(s.Problems, err.Error())
			} else {
				s.Manifest = *m
			}
			s.Problems = append(s.Problems, validate(root, path, s)...)
		} else {
			s.Problems = append(s.Problems, "no "+manifestFile)
			if missing := missingTasks(s.Tasks, requiredTasks); len(missing) > 0 {
				s.Problems = append(s.Problems, "missing tasks: "+strings.Join(missing, ", "))
			}
		}
		s.Conformant = len(s.Problems) == 0
		suites = append(suites, s)
		return nil
	})
	if err != nil {
		return nil, err
	}

	seen := map[string]*Suite{}
	for _, s := range suites {
		if s.Name == "" {
			continue
		}
		if other, ok := seen[s.Name]; ok {
			s.Problems = append(s.Problems, fmt.Sprintf("name %q is already used by %s", s.Name, other.Dir))
			s.Conformant = false
			continue
		}
		seen[s.Name] = s
	}
	return suites, nil
}

func findTaskfile(dir string) string {
	for _, name := range taskfileNames {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func loadManifest(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	// A misspelt key would otherwise be dropped without a word.
	dec.KnownFields(true)
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("%s: %w", manifestFile, err)
	}
	return &m, nil
}

// taskNames returns the sorted task names of a Taskfile.
func taskNames(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var tf struct {
		Tasks map[string]yaml.Node `yaml:"tasks"`
	}
	if err := yaml.Unmarshal(raw, &tf); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	return slices.Sorted(maps.Keys(tf.Tasks)), nil
}

func missingTasks(have, want []string) []string {
	var missing []string
	for _, t := range want {
		if !slices.Contains(have, t) {
			missing = append(missing, t)
		}
	}
	return missing
}

// validate returns what keeps a suite with a manifest from being conformant.
func validate(root, dir string, s *Suite) []string {
	var problems []string
	addf := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if !nameRe.MatchString(s.Name) {
		addf("name %q must be lowercase letters and digits separated by _ or -", s.Name)
	}
	if s.Component == "" {
		addf("component is required")
	}
	if !slices.Contains(kinds, s.Kind) {
		addf("kind %q must be one of %s", s.Kind, strings.Join(kinds, ", "))
	}
	if s.Summary == "" {
		addf("summary is required")
	}
	if s.Results != "" {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(s.Results))); err != nil {
			addf("results file %s does not exist", s.Results)
		}
	}
	if len(s.Requires.Tools) == 0 {
		addf("requires.tools is required: list at least the executables the tasks call")
	}
	for _, name := range s.Requires.Env {
		if !varRe.MatchString(name) {
			addf("requires.env %q is not an environment variable name", name)
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "README.md")); errors.Is(err, fs.ErrNotExist) {
		addf("no README.md")
	}
	if findTaskfile(dir) == "" {
		addf("no Taskfile.yaml")
	} else if missing := missingTasks(s.Tasks, requiredTasks); len(missing) > 0 {
		addf("missing tasks: %s", strings.Join(missing, ", "))
	}

	if len(s.Scenarios) == 0 {
		addf("at least one scenario is required")
	}
	seen := map[string]bool{}
	for _, sc := range s.Scenarios {
		if !nameRe.MatchString(sc.Name) {
			addf("scenario name %q must be lowercase letters and digits separated by _ or -", sc.Name)
		}
		if seen[sc.Name] {
			addf("scenario %q is declared twice", sc.Name)
		}
		seen[sc.Name] = true
		if findTaskfile(dir) != "" && !slices.Contains(s.Tasks, sc.runTask()) {
			addf("scenario %q: task %s is not in the Taskfile", sc.Name, sc.runTask())
		}
		for k := range sc.Vars {
			if !varRe.MatchString(k) {
				addf("scenario %q: var %q must be an upper-case Taskfile variable name", sc.Name, k)
			}
		}
		if sc.MaxDuration != "" {
			if d, err := time.ParseDuration(sc.MaxDuration); err != nil || d <= 0 {
				addf("scenario %q: max_duration %q is not a positive duration", sc.Name, sc.MaxDuration)
			}
		}
	}
	return problems
}

// findSuite resolves a suite by its manifest name or its directory.
func findSuite(suites []*Suite, ref string) (*Suite, error) {
	ref = strings.TrimSuffix(filepath.ToSlash(ref), "/")
	var names []string
	for _, s := range suites {
		if (s.Name != "" && s.Name == ref) || s.Dir == ref {
			return s, nil
		}
		if s.Name != "" {
			names = append(names, s.Name)
		}
	}
	return nil, fmt.Errorf("no suite %q: the suites with a manifest are %s", ref, strings.Join(names, ", "))
}
