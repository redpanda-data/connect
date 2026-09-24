// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
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

// Column widths mirror the fixed-width look of the hand-authored table so a
// generated refresh doesn't visibly reflow the document. They are cosmetic
// only — markdown tables don't require aligned pipes — so getting them
// slightly wrong is harmless.
const (
	colScenarioWidth = 21
	colVCPUWidth     = 9
	colConnectWidth  = 12
	colKCWidth       = 7
	colGapWidth      = 18
	colLastRunWidth  = 11
	// summaryColumnCount is the number of columns in the current table
	// schema (Connector/Scenario, Best vCPU, Connect MB/s, KC MB/s, Gap,
	// Last Run). An existing row with a different count is from a stale
	// schema and can't be safely retained, so it's skipped instead.
	summaryColumnCount = 6
)

func alignLeft(s string, width int) string  { return fmt.Sprintf("%-*s", width, s) }
func alignRight(s string, width int) string { return fmt.Sprintf("%*s", width, s) }

// mdRow is one fully-formatted table row, ready to drop between pipes. Both
// freshly-derived rows and rows carried forward from an existing table
// render through this the same way, so retained cell text can be treated as
// opaque and copied through untouched.
type mdRow struct {
	Scenario string
	VCPU     string
	Connect  string
	KC       string
	Gap      string
	LastRun  string
}

// toMDRow formats a freshly-derived summaryRow for display.
func toMDRow(r summaryRow) mdRow {
	vcpu := "—"
	if r.BestVCPU != 0 {
		vcpu = fmt.Sprintf("%d", r.BestVCPU)
	}
	connect := "—"
	if r.ConnectMedianMB != 0 {
		connect = fmt.Sprintf("%.0f", r.ConnectMedianMB)
	}
	kc := "—"
	if r.KCMedianMB != 0 {
		kc = fmt.Sprintf("%.0f", r.KCMedianMB)
	}
	gap := "—"
	if r.GapStr != "" {
		gap = r.GapStr
	}
	lastRun := r.LastRunDate
	if lastRun == "" {
		lastRun = "—"
	}
	return mdRow{
		Scenario: alignLeft(r.ConnectorScenario, colScenarioWidth),
		VCPU:     alignRight(vcpu, colVCPUWidth),
		Connect:  alignRight(connect, colConnectWidth),
		KC:       alignRight(kc, colKCWidth),
		Gap:      alignLeft(gap, colGapWidth),
		LastRun:  alignLeft(lastRun, colLastRunWidth),
	}
}

// retainedMarker flags a row that was carried forward from an earlier
// refresh rather than re-derived from a result JSON on disk.
const retainedMarker = "†"

// ensureRetainedMarker appends the retained marker unless it's already
// there, so re-running a refresh on a still-missing result JSON doesn't
// pile up multiple markers on the same cell.
func ensureRetainedMarker(s string) string {
	if strings.HasSuffix(s, retainedMarker) {
		return s
	}
	if s == "" {
		return retainedMarker
	}
	return s + " " + retainedMarker
}

// retainedMDRow formats a row read back from the existing table. Every cell
// except Last Run is copied through verbatim — nothing here is re-derived —
// so a scenario whose result JSON has vanished can't drift numerically.
func retainedMDRow(cells [summaryColumnCount]string) mdRow {
	return mdRow{
		Scenario: alignLeft(cells[0], colScenarioWidth),
		VCPU:     alignRight(cells[1], colVCPUWidth),
		Connect:  alignRight(cells[2], colConnectWidth),
		KC:       alignRight(cells[3], colKCWidth),
		Gap:      alignLeft(cells[4], colGapWidth),
		LastRun:  alignLeft(ensureRetainedMarker(cells[5]), colLastRunWidth),
	}
}

// summaryTmplData is the root object handed to the summary template.
type summaryTmplData struct {
	Rows          []mdRow
	LastRefreshed string
	Footnote      string
}

// renderSection writes the rendered table to w. The leading comment marker
// is NOT emitted here — RefreshSummary owns the markers because it has to
// match them exactly against existing file content.
func renderSection(w io.Writer, rows []summaryRow, lastRefreshed string) error {
	mdRows := make([]mdRow, len(rows))
	for i, r := range rows {
		mdRows[i] = toMDRow(r)
	}
	return summaryTmpl.Execute(w, summaryTmplData{Rows: mdRows, LastRefreshed: lastRefreshed})
}

// renderMergedSection is renderSection's counterpart for the merge path: the
// rows are already formatted (a mix of fresh and retained), and an optional
// footnote explains any retained rows to a reader.
func renderMergedSection(w io.Writer, rows []mdRow, lastRefreshed, footnote string) error {
	return summaryTmpl.Execute(w, summaryTmplData{Rows: rows, LastRefreshed: lastRefreshed, Footnote: footnote})
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
		// Gap is a RECORDS comparison — see the delta note in render.go. Bytes
		// are not comparable across the engines (different producer compression,
		// and Debezium's envelope is fatter per record), so a byte gap reads as a
		// speed difference when it is a verbosity difference. Older result files
		// carry MedianMsgPerSec = 0 on their KC points, because that field was
		// never populated before the records fix; those rows get no gap rather
		// than a misleading one.
		if bestConnect.Summary.MedianMsgPerSec > 0 && matchingKC.Summary.MedianMsgPerSec > 0 {
			diff := bestConnect.Summary.MedianMsgPerSec - matchingKC.Summary.MedianMsgPerSec
			pct := 100.0 * diff / bestConnect.Summary.MedianMsgPerSec
			row.GapStr = fmt.Sprintf("%+s msg/s (%+.0f%%)", formatThousands(int64(diff)), pct)
		}
	}

	// Plan 3: append a ⚠ marker if the latest result flagged cross-engine
	// divergence. The marker bubbles up so a reader scanning the SUMMARY
	// table sees at a glance which scenarios diverged.
	if len(r.CrossEngineAnomalies) > 0 {
		row.ConnectorScenario += " ⚠"
	}

	return row, nil
}

const (
	SummaryMarkerStart = "<!-- bench:aws:start - auto-generated, do not edit by hand -->"
	SummaryMarkerEnd   = "<!-- bench:aws:end -->"
)

// retainedRow is one data row read back from an existing rendered table,
// kept as opaque cell text. Nothing here is parsed into numbers, so a
// scenario whose result JSON has vanished can be carried forward without
// any chance of the displayed figures drifting from what was published.
type retainedRow struct {
	Key   string // the ConnectorScenario cell, exactly as rendered before
	Cells [summaryColumnCount]string
}

// parseExistingRows scans the markdown already rendered between the
// summary markers and extracts its data rows verbatim. Header, separator,
// the "no AWS runs yet" placeholder, and anything after the table (a
// footnote, trailing prose) are all skipped. Rows whose column count
// doesn't match the current schema are skipped with a warning rather than
// being mangled or crashing the refresh.
func parseExistingRows(block string) (rows []retainedRow, warnings []string) {
	lines := strings.Split(block, "\n")
	inTable := false
	pastSeparator := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !inTable {
			if strings.HasPrefix(trimmed, "| Connector / Scenario") {
				inTable = true
			}
			continue
		}
		if !pastSeparator {
			pastSeparator = true // this line is the header's "---" separator row
			continue
		}
		if !strings.HasPrefix(trimmed, "|") {
			break // table ended
		}
		cells := splitRowCells(trimmed)
		if len(cells) > 0 && strings.Contains(cells[0], "no AWS runs yet") {
			continue
		}
		if len(cells) != summaryColumnCount {
			warnings = append(warnings, fmt.Sprintf(
				"skipping existing row with %d column(s), want %d: %q",
				len(cells), summaryColumnCount, trimmed))
			continue
		}
		var cellArr [summaryColumnCount]string
		copy(cellArr[:], cells)
		rows = append(rows, retainedRow{Key: cells[0], Cells: cellArr})
	}
	return rows, warnings
}

// splitRowCells splits one markdown table row into trimmed cell strings,
// dropping the empty fields produced by the leading and trailing pipes.
func splitRowCells(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	parts := strings.Split(line, "|")
	cells := make([]string, len(parts))
	for i, p := range parts {
		cells[i] = strings.TrimSpace(p)
	}
	return cells
}

// baseKey strips the cross-engine-divergence ⚠ suffix so a row can be
// matched between an existing table and a freshly-derived one even if its
// divergence status changed between refreshes.
func baseKey(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "⚠")
	return strings.TrimSpace(s)
}

// mergeRows combines freshly-derived rows with rows read back from the
// existing table. Fresh always wins for a given scenario; an existing row
// is retained only when no fresh row exists for it, which is exactly the
// "result JSON no longer on disk" case this whole merge exists to protect
// against. The merged set stays sorted alphabetically by scenario, matching
// walkResults' own ordering.
func mergeRows(fresh []summaryRow, existingRows []retainedRow) (rows []mdRow, anyRetained bool) {
	type keyed struct {
		sortKey string
		row     mdRow
	}
	all := make([]keyed, 0, len(fresh)+len(existingRows))
	freshBase := make(map[string]bool, len(fresh))
	for _, r := range fresh {
		freshBase[baseKey(r.ConnectorScenario)] = true
		all = append(all, keyed{sortKey: r.ConnectorScenario, row: toMDRow(r)})
	}
	for _, ex := range existingRows {
		if freshBase[baseKey(ex.Key)] {
			continue // a fresh row supersedes the retained one
		}
		anyRetained = true
		all = append(all, keyed{sortKey: ex.Key, row: retainedMDRow(ex.Cells)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].sortKey < all[j].sortKey })
	rows = make([]mdRow, len(all))
	for i, k := range all {
		rows[i] = k.row
	}
	return rows, anyRetained
}

// retainedFootnote is appended under the table whenever at least one row
// was carried forward, so a reader isn't misled into thinking a retained
// row was freshly measured.
const retainedFootnote = "† Retained from an earlier refresh — this scenario's result JSON is no longer on disk, so the row could not be re-derived."

// findAllIndices returns the start offset of every non-overlapping
// occurrence of sub within data.
func findAllIndices(data, sub []byte) []int {
	var idxs []int
	offset := 0
	for {
		i := bytes.Index(data[offset:], sub)
		if i < 0 {
			break
		}
		idxs = append(idxs, offset+i)
		offset += i + len(sub)
	}
	return idxs
}

// lineNumberAt returns the 1-indexed line number containing byte offset.
func lineNumberAt(data []byte, offset int) int {
	return bytes.Count(data[:offset], []byte("\n")) + 1
}

// duplicateMarkerError builds the operator-facing error for defect 2: more
// than one bench:aws block in the document means only the first one would
// ever get refreshed, silently freezing the rest. Refusing to guess which
// block is "the real one" and instead naming every occurrence forces a
// single human decision instead of an auto-deletion.
func duplicateMarkerError(path string, data []byte, startIdxs, endIdxs []int) error {
	var b strings.Builder
	fmt.Fprintf(&b, "multiple bench:aws marker blocks found in %s; delete the extra block(s) so only one remains, then re-run:\n", path)
	for _, idx := range startIdxs {
		fmt.Fprintf(&b, "  start marker at line %d (byte offset %d)\n", lineNumberAt(data, idx), idx)
	}
	for _, idx := range endIdxs {
		fmt.Fprintf(&b, "  end marker at line %d (byte offset %d)\n", lineNumberAt(data, idx), idx)
	}
	return errors.New(strings.TrimRight(b.String(), "\n"))
}

// RefreshSummary walks resultsRoot, derives the latest-per-scenario rows,
// merges them with whatever is already published between the markers in
// summaryPath, and writes the merged table back. A scenario whose result
// JSON is no longer on disk keeps its previously published row (marked with
// †) instead of being silently dropped.
//
// If markers are missing entirely, the section is appended to the end of
// the file and a one-line warning is written to os.Stderr, exactly as
// before — there's no existing table to merge against in that case. If more
// than one start or end marker is found, RefreshSummary refuses to guess
// which block is authoritative and returns an error naming every occurrence
// without modifying the file.
//
// The write is atomic: tmp file + rename, mirroring WriteResultJSON.
func RefreshSummary(summaryPath, resultsRoot string, now time.Time) error {
	rows, err := walkResults(resultsRoot)
	if err != nil {
		return fmt.Errorf("walk results: %w", err)
	}

	existing, err := os.ReadFile(summaryPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", summaryPath, err)
	}

	startIdxs := findAllIndices(existing, []byte(SummaryMarkerStart))
	endIdxs := findAllIndices(existing, []byte(SummaryMarkerEnd))
	if len(startIdxs) > 1 || len(endIdxs) > 1 {
		return duplicateMarkerError(summaryPath, existing, startIdxs, endIdxs)
	}

	nowStr := now.UTC().Format("2006-01-02")

	var next []byte
	switch {
	case len(startIdxs) == 1 && len(endIdxs) == 1 && endIdxs[0] > startIdxs[0]:
		startIdx, endIdx := startIdxs[0], endIdxs[0]
		blockContent := string(existing[startIdx+len(SummaryMarkerStart) : endIdx])
		existingRows, warnings := parseExistingRows(blockContent)
		for _, w := range warnings {
			fmt.Fprintln(os.Stderr, "warning: "+w)
		}

		merged, anyRetained := mergeRows(rows, existingRows)
		footnote := ""
		if anyRetained {
			footnote = retainedFootnote
		}

		var section bytes.Buffer
		section.WriteString(SummaryMarkerStart)
		section.WriteByte('\n')
		if err := renderMergedSection(&section, merged, nowStr, footnote); err != nil {
			return fmt.Errorf("render section: %w", err)
		}
		section.WriteByte('\n')
		section.WriteString(SummaryMarkerEnd)

		endTotal := endIdx + len(SummaryMarkerEnd)
		next = append(next, existing[:startIdx]...)
		next = append(next, section.Bytes()...)
		next = append(next, existing[endTotal:]...)
	default:
		var section bytes.Buffer
		section.WriteString(SummaryMarkerStart)
		section.WriteByte('\n')
		if err := renderSection(&section, rows, nowStr); err != nil {
			return fmt.Errorf("render section: %w", err)
		}
		section.WriteByte('\n')
		section.WriteString(SummaryMarkerEnd)

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
