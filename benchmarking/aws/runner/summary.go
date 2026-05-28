// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
	"time"
)

// summaryRow is one line in the auto-rendered table.
type summaryRow struct {
	ConnectorScenario string  // "postgres / orders-cdc"
	BestVCPU          int     // 0 sentinel when no points
	ConnectMedianMB   float64 // Connect's median at BestVCPU
	KCMedianMB        float64 // KC's median at the same vCPU; 0 if KC didn't run
	GapStr            string  // "+28 MB/s (+39%)" — Connect minus KC; blank when KC absent
	LastRunDate       string  // YYYY-MM-DD
	ResultJSONPath    string  // relative to repo root, for footnote linking
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

	// Split by engine, pick the best Connect point, find the matching KC
	// point at the same vCPU. The KC point may not exist (single-engine run
	// or KC never made it to that vCPU).
	var connectPts, kcPts []PointResult
	for _, p := range r.Points {
		switch p.Engine {
		case "connect", "": // empty-engine results pre-date Plan 2 — treat as connect
			connectPts = append(connectPts, p)
		case "kafka_connect":
			kcPts = append(kcPts, p)
		}
	}
	var bestConnect PointResult
	for _, p := range connectPts {
		if p.Summary.MedianMBPerSec > bestConnect.Summary.MedianMBPerSec {
			bestConnect = p
		}
	}
	row.BestVCPU = bestConnect.VCPU
	row.ConnectMedianMB = bestConnect.Summary.MedianMBPerSec

	var matchingKC PointResult
	for _, p := range kcPts {
		if p.VCPU == bestConnect.VCPU {
			matchingKC = p
			break
		}
	}
	if matchingKC.Engine != "" {
		row.KCMedianMB = matchingKC.Summary.MedianMBPerSec
		if bestConnect.Summary.MedianMBPerSec > 0 {
			diff := bestConnect.Summary.MedianMBPerSec - matchingKC.Summary.MedianMBPerSec
			pct := 100.0 * diff / bestConnect.Summary.MedianMBPerSec
			row.GapStr = fmt.Sprintf("%+.0f MB/s (%+.0f%%)", diff, pct)
		}
	}
	return row, nil
}

const (
	SummaryMarkerStart = "<!-- bench:aws:start - auto-generated, do not edit by hand -->"
	SummaryMarkerEnd   = "<!-- bench:aws:end -->"
)

// RefreshSummary walks resultsRoot, derives the latest-per-scenario rows,
// renders the section, and writes it between the markers in summaryPath.
// If markers are missing, the section is appended to the end of the file
// and a one-line warning is written to os.Stderr.
//
// The write is atomic: tmp file + rename, mirroring WriteResultJSON.
func RefreshSummary(summaryPath, resultsRoot string, now time.Time) error {
	rows, err := walkResults(resultsRoot)
	if err != nil {
		return fmt.Errorf("walk results: %w", err)
	}

	var section bytes.Buffer
	section.WriteString(SummaryMarkerStart)
	section.WriteByte('\n')
	if err := renderSection(&section, rows, now.UTC().Format("2006-01-02")); err != nil {
		return fmt.Errorf("render section: %w", err)
	}
	section.WriteByte('\n')
	section.WriteString(SummaryMarkerEnd)

	existing, err := os.ReadFile(summaryPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", summaryPath, err)
	}

	var next []byte
	startIdx := bytes.Index(existing, []byte(SummaryMarkerStart))
	endIdx := bytes.Index(existing, []byte(SummaryMarkerEnd))
	switch {
	case startIdx >= 0 && endIdx > startIdx:
		endTotal := endIdx + len(SummaryMarkerEnd)
		next = append(next, existing[:startIdx]...)
		next = append(next, section.Bytes()...)
		next = append(next, existing[endTotal:]...)
	default:
		fmt.Fprintln(os.Stderr, "warning: bench:aws markers not found in "+summaryPath+"; appending section to end of file")
		next = append(next, existing...)
		if !strings.HasSuffix(string(existing), "\n") {
			next = append(next, '\n')
		}
		next = append(next, '\n')
		next = append(next, section.Bytes()...)
		next = append(next, '\n')
	}

	tmp := summaryPath + ".tmp"
	if err := os.WriteFile(tmp, next, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, summaryPath); err != nil {
		return fmt.Errorf("rename tmp: %w", err)
	}
	return nil
}
