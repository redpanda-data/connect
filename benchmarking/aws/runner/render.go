// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

// Result is the canonical per-run artefact, written as JSON and rendered to
// markdown.
type Result struct {
	Scenario     string        `json:"scenario"`
	ScenarioHash string        `json:"scenario_hash"`
	GitSHA       string        `json:"git_sha"`
	StartedAt    time.Time     `json:"started_at"`
	FinishedAt   time.Time     `json:"finished_at"`
	Infra        ResultInfra   `json:"infra"`
	Dataset      ResultDataset `json:"dataset"`
	Points       []PointResult `json:"points"`
}

type ResultInfra struct {
	RunnerInstanceType  string `json:"runner_instance_type"`
	SourceInstanceClass string `json:"source_instance_class"`
	SourceStorageGB     int    `json:"source_storage_gb"`
	Region              string `json:"region"`
}

type ResultDataset struct {
	Rows         int64 `json:"rows"`
	RowSizeBytes int   `json:"row_size_bytes"`
	TotalBytes   int64 `json:"total_bytes"`
}

type PointResult struct {
	VCPU         int                `json:"vcpu"`
	Samples      []Sample           `json:"samples"`
	Summary      Summary            `json:"summary"`
	Anomalies    []Anomaly          `json:"anomalies,omitempty"`
	PromSnapshot map[string]float64 `json:"prom_snapshot,omitempty"`
}

// WriteResultJSON writes the result to <dir>/<connector>/<scenario>/<timestamp>.json.
// Connector and scenario are inferred from r.Scenario (format "connector/scenario").
func WriteResultJSON(dir string, r *Result) (string, error) {
	parts := strings.SplitN(r.Scenario, "/", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("scenario %q is not connector/scenario", r.Scenario)
	}
	outDir := filepath.Join(dir, parts[0], parts[1])
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	stamp := r.StartedAt.UTC().Format("2006-01-02T15-04-05Z")
	out := filepath.Join(outDir, stamp+".json")
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(out, raw, 0o644); err != nil {
		return "", err
	}
	return out, nil
}

//go:embed templates/result.md.tmpl
var resultMarkdownTmpl string

type markdownView struct {
	ScenarioShort string
	Date          string
	Description   string
	GitSHA        string
	GitShortSHA   string
	Infra         ResultInfra
	Dataset       ResultDataset
	WorkloadLine  string
	Rows          []markdownRow
	Anomalies     []anomalyView
	JSONPath      string
}

type markdownRow struct {
	VCPU        int
	MedianMB    string
	P5MB        string
	P95MB       string
	MedianMsgFC string
}

type anomalyView struct {
	VCPU        int
	DurationSec int
	MinRatio    float64
	StartT      int
}

// AppendMarkdown appends a results section to the target file.
// The description argument is the scenario's prose description.
func AppendMarkdown(target string, r *Result, description string) error {
	parts := strings.SplitN(r.Scenario, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("scenario %q is not connector/scenario", r.Scenario)
	}

	rows := make([]markdownRow, len(r.Points))
	var anomalies []anomalyView
	for i, p := range r.Points {
		rows[i] = markdownRow{
			VCPU:        p.VCPU,
			MedianMB:    fmt.Sprintf("%12.0f", p.Summary.MedianMBPerSec),
			P5MB:        fmt.Sprintf("%11.0f", p.Summary.P5MBPerSec),
			P95MB:       fmt.Sprintf("%12.0f", p.Summary.P95MBPerSec),
			MedianMsgFC: formatThousands(int64(p.Summary.MedianMsgPerSec)),
		}
		for _, a := range p.Anomalies {
			anomalies = append(anomalies, anomalyView{
				VCPU: p.VCPU, DurationSec: a.DurationSec, MinRatio: a.MinRatio, StartT: a.StartT,
			})
		}
	}

	workload := ""
	if r.Dataset.Rows > 0 {
		workload = fmt.Sprintf("%s rows × %d B = ~%d GB",
			formatThousands(r.Dataset.Rows), r.Dataset.RowSizeBytes, r.Dataset.TotalBytes/(1<<30))
	}

	gitShort := r.GitSHA
	if len(gitShort) > 9 {
		gitShort = gitShort[:9]
	}

	view := markdownView{
		ScenarioShort: parts[1],
		Date:          r.StartedAt.UTC().Format("2006-01-02"),
		Description:   description,
		GitSHA:        r.GitSHA,
		GitShortSHA:   gitShort,
		Infra:         r.Infra,
		Dataset:       r.Dataset,
		WorkloadLine:  workload,
		Rows:          rows,
		Anomalies:     anomalies,
		JSONPath:      fmt.Sprintf("results/%s/%s/%s.json", parts[0], parts[1], r.StartedAt.UTC().Format("2006-01-02T15-04-05Z")),
	}

	t, err := template.New("result").Parse(resultMarkdownTmpl)
	if err != nil {
		return err
	}
	var sb strings.Builder
	if err := t.Execute(&sb, view); err != nil {
		return err
	}

	f, err := os.OpenFile(target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString("\n\n"); err != nil {
		return err
	}
	_, err = f.WriteString(sb.String())
	return err
}

func formatThousands(n int64) string {
	in := fmt.Sprintf("%d", n)
	if len(in) <= 3 {
		return in
	}
	var sb strings.Builder
	pre := len(in) % 3
	if pre > 0 {
		sb.WriteString(in[:pre])
		if len(in) > pre {
			sb.WriteString(",")
		}
	}
	for i := pre; i < len(in); i += 3 {
		sb.WriteString(in[i : i+3])
		if i+3 < len(in) {
			sb.WriteString(",")
		}
	}
	return sb.String()
}
