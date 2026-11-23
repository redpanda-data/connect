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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/benchmarking/aws/catalog"
)

// resultFile is the subset of the runner's result JSON the importer reads.
// The runner package is package main and cannot be imported.
type resultFile struct {
	Scenario   string    `json:"scenario"`
	GitSHA     string    `json:"git_sha"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Infra      struct {
		RunnerInstanceType string `json:"runner_instance_type"`
		Region             string `json:"region"`
	} `json:"infra"`
	Dataset struct {
		RowSizeBytes int `json:"row_size_bytes"`
	} `json:"dataset"`
	Points []struct {
		VCPU    int    `json:"vcpu"`
		Engine  string `json:"engine"`
		Arm     string `json:"arm"`
		Binary  string `json:"binary"`
		Summary struct {
			MedianMBPerSec  float64 `json:"median_mb_s"`
			MedianMsgPerSec float64 `json:"median_msg_s"`
		} `json:"summary"`
		Prom []struct {
			RSSBytes uint64 `json:"rss_bytes"`
		} `json:"prom"`
	} `json:"points"`
}

// scenarioFile is the subset of the runner's scenario YAML the importer reads.
type scenarioFile struct {
	Direction string         `yaml:"direction"`
	Soak      bool           `yaml:"soak"`
	Pipeline  map[string]any `yaml:"pipeline"`
	Workload  struct {
		WriteRatePerSec int `yaml:"write_rate_per_sec"`
	} `yaml:"workload"`
	Infra struct {
		MemoryPerVCPUBytes int64 `yaml:"memory_per_vcpu_bytes"`
	} `yaml:"infra"`
}

type skipped struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// describeFunc resolves a build SHA to its release-describe string.
type describeFunc func(sha string) (string, error)

func gitDescribeIn(repo string) describeFunc {
	return func(sha string) (string, error) {
		out, err := exec.Command("git", "-C", repo, "describe", "--tags", "--match", "v4.*", sha).Output()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(out)), nil
	}
}

// scenarioAtFunc returns a scenario's YAML as it was at a commit. scenario is
// "<connector>/<scenario>".
type scenarioAtFunc func(sha, scenario string) ([]byte, error)

// gitScenarioAt reads <scenariosRoot>/<scenario>.yaml at sha from repo, so
// the importer sees the scenario as it was when the result was produced.
func gitScenarioAt(repo, scenariosRoot string) scenarioAtFunc {
	return func(sha, scenario string) ([]byte, error) {
		return exec.Command("git", "-C", repo, "show", sha+":"+path.Join(scenariosRoot, scenario)+".yaml").Output()
	}
}

// validSHA guards what reaches git: hex only, abbreviated or full.
var validSHA = regexp.MustCompile(`^[0-9a-fA-F]{7,40}$`)

func runImport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("import", stderr)
	store := fs.String("store", defaultStore, "catalogue store: s3://<bucket> or dir:<path>")
	repo := fs.String("repo", ".", "git checkout used to read scenarios and describe build SHAs")
	resultsRoot := fs.String("results-root", "", "directory laid out as <connector>/<scenario>/*.json")
	scenariosRoot := fs.String("scenarios-root", "benchmarking/aws/scenarios",
		"repo-relative directory holding <connector>/<scenario>.yaml, read at each result's git_sha")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *resultsRoot == "" {
		return usageErr("import needs --results-root")
	}
	root := path.Clean(filepath.ToSlash(*scenariosRoot))
	if *scenariosRoot == "" || path.IsAbs(root) || root == ".." || strings.HasPrefix(root, "../") {
		return usageErr("--scenarios-root %q: want a path relative to the --repo root", *scenariosRoot)
	}
	st, err := openStore(ctx, *store)
	if err != nil {
		return err
	}
	files, err := walkResultFiles(*resultsRoot)
	if err != nil {
		return err
	}

	out := struct {
		Imported []string  `json:"imported"`
		Skipped  []skipped `json:"skipped"`
	}{Imported: []string{}, Skipped: []skipped{}}
	describe := gitDescribeIn(*repo)
	scenarioAt := gitScenarioAt(*repo, root)
	for _, f := range files {
		e, reason := entryFromResultFile(f, *resultsRoot, scenarioAt, describe)
		if reason != "" {
			out.Skipped = append(out.Skipped, skipped{Path: f, Reason: reason})
			continue
		}
		if err := st.Put(ctx, e); err != nil {
			return fmt.Errorf("storing %s: %w", e.ID, err)
		}
		out.Imported = append(out.Imported, e.ID)
	}
	writeJSON(stdout, out)
	return nil
}

// walkResultFiles lists <root>/<connector>/<scenario>/*.json in order.
func walkResultFiles(root string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(root, "*", "*", "*.json"))
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(root); err != nil {
		return nil, fmt.Errorf("results root: %w", err)
	}
	sort.Strings(files)
	return files, nil
}

// entryFromResultFile builds the entry for one result file, or returns the
// reason it was skipped. One bad file never aborts the import.
func entryFromResultFile(file, resultsRoot string, scenarioAt scenarioAtFunc, describe describeFunc) (catalog.Entry, string) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return catalog.Entry{}, err.Error()
	}
	var res resultFile
	if err := json.Unmarshal(raw, &res); err != nil {
		return catalog.Entry{}, fmt.Sprintf("decoding result: %v", err)
	}
	if !validSHA.MatchString(res.GitSHA) {
		return catalog.Entry{}, "build sha unknown"
	}
	if res.Scenario == "" {
		return catalog.Entry{}, "scenario yaml missing"
	}
	scenRaw, err := scenarioAt(res.GitSHA, res.Scenario)
	if err != nil {
		return catalog.Entry{}, "scenario yaml missing"
	}
	var scen scenarioFile
	if err := yaml.Unmarshal(scenRaw, &scen); err != nil {
		return catalog.Entry{}, fmt.Sprintf("decoding scenario: %v", err)
	}
	if scen.Direction == "sink" {
		return catalog.Entry{}, "sink direction not supported"
	}
	version, err := describe(res.GitSHA)
	if err != nil || version == "" {
		return catalog.Entry{}, "connect version unknown"
	}

	rel, err := filepath.Rel(resultsRoot, file)
	if err != nil {
		return catalog.Entry{}, err.Error()
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	stamp := strings.TrimSuffix(parts[len(parts)-1], ".json")
	now := res.FinishedAt
	if now.IsZero() {
		now = res.StartedAt
	}
	run := catalog.RunInput{
		ID:                strings.Join(append(parts[:len(parts)-1:len(parts)-1], stamp), "-"),
		Scenario:          res.Scenario,
		GitSHA:            res.GitSHA,
		ConnectVersion:    version,
		InstanceType:      res.Infra.RunnerInstanceType,
		Region:            res.Infra.Region,
		Pipeline:          catalog.BenchSourcePipeline(scen.Pipeline),
		MessageSizeBytes:  res.Dataset.RowSizeBytes,
		OfferedRatePerSec: scen.Workload.WriteRatePerSec,
		Soak:              scen.Soak,
		MemoryLimited:     scen.Infra.MemoryPerVCPUBytes > 0,
		SinkMeasured:      true,
		Now:               now,
	}
	for _, p := range res.Points {
		rp := catalog.RunPoint{
			VCPU: p.VCPU, Engine: p.Engine, Arm: p.Arm, Binary: p.Binary,
			MedianMBps: p.Summary.MedianMBPerSec, MedianMsgps: p.Summary.MedianMsgPerSec,
		}
		for _, pp := range p.Prom {
			rp.RSSBytes = append(rp.RSSBytes, pp.RSSBytes)
		}
		run.Points = append(run.Points, rp)
	}

	e, err := catalog.EntryFromRun(run)
	if err != nil {
		return catalog.Entry{}, err.Error()
	}
	return e, ""
}
