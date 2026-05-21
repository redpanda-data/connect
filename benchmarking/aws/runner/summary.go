// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"text/template"
)

// summaryRow is one line in the auto-rendered table.
type summaryRow struct {
	ConnectorScenario string  // "postgres / orders-cdc"
	PeakMBPerSec      float64 // 0 if every point was zero
	BestVCPU          int     // 0 sentinel when peak is 0
	MedianAtBestMB    float64
	LastRunDate       string // YYYY-MM-DD
	ResultJSONPath    string // relative to repo root, for footnote linking
}

// walkResults discovers every <root>/<connector>/<scenario>/*.json result
// file, picks the newest per (connector, scenario), and derives one
// summaryRow per scenario sorted alphabetically by ConnectorScenario.
func walkResults(root string) ([]summaryRow, error) {
	connectors, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read results root %s: %w", root, err)
	}
	var rows []summaryRow
	for _, c := range connectors {
		if !c.IsDir() {
			continue
		}
		connDir := filepath.Join(root, c.Name())
		scenarios, err := os.ReadDir(connDir)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", connDir, err)
		}
		for _, s := range scenarios {
			if !s.IsDir() {
				continue
			}
			scenDir := filepath.Join(connDir, s.Name())
			jsons, err := filepath.Glob(filepath.Join(scenDir, "*.json"))
			if err != nil {
				return nil, fmt.Errorf("glob %s: %w", scenDir, err)
			}
			if len(jsons) == 0 {
				continue
			}
			sort.Strings(jsons) // timestamp prefix → lexicographic == chronological
			latest := jsons[len(jsons)-1]
			row, err := derivedRow(c.Name(), s.Name(), latest)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ConnectorScenario < rows[j].ConnectorScenario })
	return rows, nil
}

//go:embed templates/summary-section.md.tmpl
var summaryTmplSrc string

var summaryTmpl = template.Must(template.New("summary").Parse(summaryTmplSrc))

// renderSection writes the rendered table to w. The leading comment marker
// is NOT emitted here — RefreshSummary owns the markers because it has to
// match them exactly against existing file content.
func renderSection(w io.Writer, rows []summaryRow, lastRefreshed string) error {
	return summaryTmpl.Execute(w, struct {
		Rows          []summaryRow
		LastRefreshed string
	}{Rows: rows, LastRefreshed: lastRefreshed})
}

// derivedRow loads one Result JSON and returns the summary row for it.
func derivedRow(connector, scenario, jsonPath string) (summaryRow, error) {
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		return summaryRow{}, fmt.Errorf("read %s: %w", jsonPath, err)
	}
	var r Result
	if err := json.Unmarshal(raw, &r); err != nil {
		return summaryRow{}, fmt.Errorf("parse %s: %w", jsonPath, err)
	}
	row := summaryRow{
		ConnectorScenario: connector + " / " + scenario,
		LastRunDate:       r.FinishedAt.UTC().Format("2006-01-02"),
		ResultJSONPath:    jsonPath,
	}
	for _, p := range r.Points {
		if p.Summary.PeakMBPerSec > row.PeakMBPerSec {
			row.PeakMBPerSec = p.Summary.PeakMBPerSec
			row.BestVCPU = p.VCPU
			row.MedianAtBestMB = p.Summary.MedianMBPerSec
		}
	}
	return row, nil
}
