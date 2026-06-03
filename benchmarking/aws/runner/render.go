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
	"sort"
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

	// CrossEngineAnomalies flags vCPU points where Connect and KC
	// diverged by more than a configured ratio. Populated by runBench
	// after both engines' points are gathered; empty for single-engine
	// runs.
	CrossEngineAnomalies []CrossEngineAnomaly `json:"cross_engine_anomalies,omitempty"`
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
	VCPU      int         `json:"vcpu"`
	Engine    string      `json:"engine"`
	Samples   []Sample    `json:"samples"`
	Summary   Summary     `json:"summary"`
	Anomalies []Anomaly   `json:"anomalies,omitempty"`
	Prom      []PromPoint `json:"prom,omitempty"`

	// BrokerSeries is the broker-side throughput attributed to this engine
	// at this vCPU point. For Connect, it's a cross-check against the
	// rolling-stats-derived Summary. For KC (which has no rolling-stats
	// line to parse), Summary is derived from this series — see Task 6.
	BrokerSeries []TopicPoint `json:"broker_series,omitempty"`
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
	ScenarioShort        string
	Date                 string
	Description          string
	GitSHA               string
	GitShortSHA          string
	Infra                ResultInfra
	Dataset              ResultDataset
	WorkloadLine         string
	Rows                 []markdownRow
	Anomalies            []anomalyView
	CrossEngineAnomalies []CrossEngineAnomaly // populated for dual-engine runs
	JSONPath             string
}

type markdownRow struct {
	VCPU           int
	Engine         string
	MedianMB       string
	MeanMB         string
	BrokerMedianMB string
	P5MB           string
	P95MB          string
	MedianMsgFC    string
	DeltaVsConnect string // blank for connect rows; "+3 MB/s (+10%)" or similar for KC rows
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

	// First pass: group points by vCPU and capture iteration order.
	type vGroup struct {
		vcpu     int
		byEngine map[string]PointResult
	}
	groups := map[int]*vGroup{}
	var order []int
	for _, p := range r.Points {
		g, ok := groups[p.VCPU]
		if !ok {
			g = &vGroup{vcpu: p.VCPU, byEngine: map[string]PointResult{}}
			groups[p.VCPU] = g
			order = append(order, p.VCPU)
		}
		g.byEngine[p.Engine] = p
	}

	// Second pass: emit one row per (vcpu, engine) — connect first, then kafka_connect.
	// For KC rows, compute the delta column from the matching Connect row.
	var rows []markdownRow
	var anomalies []anomalyView
	for _, vcpu := range order {
		g := groups[vcpu]
		for _, engine := range []string{"connect", "kafka_connect"} {
			p, ok := g.byEngine[engine]
			if !ok {
				continue
			}
			var brokerMedian float64
			if len(p.BrokerSeries) > 0 {
				rates := make([]float64, len(p.BrokerSeries))
				for j, b := range p.BrokerSeries {
					rates[j] = b.MBPerSec
				}
				sort.Float64s(rates)
				brokerMedian = rates[len(rates)/2]
			}
			deltaStr := ""
			if engine == "kafka_connect" {
				if connectPt, hasConnect := g.byEngine["connect"]; hasConnect && connectPt.Summary.MedianMBPerSec > 0 {
					diff := p.Summary.MedianMBPerSec - connectPt.Summary.MedianMBPerSec
					pct := 100.0 * diff / connectPt.Summary.MedianMBPerSec
					deltaStr = fmt.Sprintf("%+.0f MB/s (%+.0f%%)", diff, pct)
				}
			}
			rows = append(rows, markdownRow{
				VCPU:           p.VCPU,
				Engine:         p.Engine,
				MedianMB:       fmt.Sprintf("%12.0f", p.Summary.MedianMBPerSec),
				MeanMB:         fmt.Sprintf("%12.3f", p.Summary.MeanMBPerSec),
				BrokerMedianMB: fmt.Sprintf("%12.0f", brokerMedian),
				P5MB:           fmt.Sprintf("%11.0f", p.Summary.P5MBPerSec),
				P95MB:          fmt.Sprintf("%12.0f", p.Summary.P95MBPerSec),
				MedianMsgFC:    formatThousands(int64(p.Summary.MedianMsgPerSec)),
				DeltaVsConnect: deltaStr,
			})
			for _, a := range p.Anomalies {
				anomalies = append(anomalies, anomalyView{
					VCPU: p.VCPU, DurationSec: a.DurationSec, MinRatio: a.MinRatio, StartT: a.StartT,
				})
			}
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
		ScenarioShort:        parts[1],
		Date:                 r.StartedAt.UTC().Format("2006-01-02"),
		Description:          description,
		GitSHA:               r.GitSHA,
		GitShortSHA:          gitShort,
		Infra:                r.Infra,
		Dataset:              r.Dataset,
		WorkloadLine:         workload,
		Rows:                 rows,
		Anomalies:            anomalies,
		CrossEngineAnomalies: r.CrossEngineAnomalies,
		JSONPath:             fmt.Sprintf("results/%s/%s/%s.json", parts[0], parts[1], r.StartedAt.UTC().Format("2006-01-02T15-04-05Z")),
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
