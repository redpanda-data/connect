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
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/v4/benchmarking/aws/catalog"
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

func runImport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("import", stderr)
	store := fs.String("store", defaultStore, "catalogue store: s3://<bucket> or dir:<path>")
	repo := fs.String("repo", ".", "git checkout used to describe build SHAs")
	resultsRoot := fs.String("results-root", "", "directory laid out as <connector>/<scenario>/*.json")
	scenariosRoot := fs.String("scenarios-root", "", "directory holding <connector>/<scenario>.yaml")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *resultsRoot == "" || *scenariosRoot == "" {
		return usageErr("import needs --results-root and --scenarios-root")
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
	for _, f := range files {
		e, reason, err := entryFromResultFile(f, *resultsRoot, *scenariosRoot, describe)
		if err != nil {
			return err
		}
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

// importErrReasons maps catalog.EntryFromRun's rejection errors to the
// importer's skip reasons.
var importErrReasons = []string{"build sha unknown", "runner instance type missing", "soak result", "arm/binary result"}

// entryFromResultFile builds the entry for one result file. A non-empty
// reason means the file was skipped; err is reserved for failures that should
// abort the import.
func entryFromResultFile(path, resultsRoot, scenariosRoot string, describe describeFunc) (e catalog.Entry, reason string, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return e, "", err
	}
	var res resultFile
	if err := json.Unmarshal(raw, &res); err != nil {
		return e, "", fmt.Errorf("decoding %s: %w", path, err)
	}

	scenRaw, err := os.ReadFile(filepath.Join(scenariosRoot, filepath.FromSlash(res.Scenario)+".yaml"))
	if err != nil || res.Scenario == "" {
		return e, "scenario yaml missing", nil
	}
	var scen scenarioFile
	if err := yaml.Unmarshal(scenRaw, &scen); err != nil {
		return e, "", fmt.Errorf("decoding scenario for %s: %w", path, err)
	}
	if scen.Direction == "sink" {
		return e, "sink direction not supported", nil
	}

	// Same shape the runner's sourceTopology emits.
	pipeline := map[string]any{
		"input": scen.Pipeline["input"],
		"output": map[string]any{
			"processors": []any{map[string]any{"benchmark": map[string]any{"interval": "1s", "count_bytes": true}}},
			"redpanda":   map[string]any{"topic": ""},
		},
	}
	if p, ok := scen.Pipeline["pipeline"]; ok {
		pipeline["pipeline"] = p
	}

	rel, err := filepath.Rel(resultsRoot, path)
	if err != nil {
		return e, "", err
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
		InstanceType:      res.Infra.RunnerInstanceType,
		Region:            res.Infra.Region,
		Pipeline:          pipeline,
		MessageSizeBytes:  res.Dataset.RowSizeBytes,
		OfferedRatePerSec: scen.Workload.WriteRatePerSec,
		Soak:              scen.Soak,
		MemoryLimited:     scen.Infra.MemoryPerVCPUBytes > 0,
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

	e, err = catalog.EntryFromRun(run)
	if err != nil {
		for _, r := range importErrReasons {
			if err.Error() == r {
				return catalog.Entry{}, r, nil
			}
		}
		return catalog.Entry{}, "", fmt.Errorf("%s: %w", path, err)
	}
	version, err := describe(res.GitSHA)
	if err != nil || version == "" {
		return catalog.Entry{}, "connect version unknown", nil
	}
	e.ConnectVersion = version
	return e, "", nil
}
