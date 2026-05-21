// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// PromPoint is one curated snapshot of Connect's runtime metrics, sampled at
// time T (seconds since end-of-warmup, matching Sample.T).
type PromPoint struct {
	T              int     `json:"t"`
	Goroutines     int     `json:"goroutines"`
	HeapInUseMB    float64 `json:"heap_in_use_mb"`
	BytesTotal     float64 `json:"bytes_total"`       // benchmark_bytes_total
	CPUSeconds     float64 `json:"cpu_seconds"`
	GCPauseTotalNS uint64  `json:"gc_pause_total_ns"` // monotonic; per-interval delta = scrape[i] - scrape[i-1]
}

// promSnapshot is one /metrics dump bracketed by ###timestamp= markers.
// The parser produces a slice of these before the curated extractor folds
// them into []PromPoint.
type promSnapshot struct {
	UnixTime int64
	Body     string
	Errored  bool // true when the scraper logged ###scrape_error inside the snapshot
}

// extractPromPoint pulls the curated metrics out of a single snapshot
// body. Returns ok=false if the snapshot was an error frame.
//
// Hand-rolled rather than depending on prometheus/common/expfmt: the
// curated subset is five unlabeled metrics, and avoiding the dependency
// keeps the runner binary small and the build graph simple.
func extractPromPoint(s promSnapshot) (PromPoint, bool) {
	if s.Errored {
		return PromPoint{}, false
	}
	pp := PromPoint{}
	scanner := bufio.NewScanner(strings.NewReader(s.Body))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, valueStr, ok := splitMetricLine(line)
		if !ok {
			continue
		}
		switch name {
		case "go_goroutines":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.Goroutines = int(n)
		case "go_memstats_heap_inuse_bytes":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.HeapInUseMB = n / 1_000_000 // base-10 to match the rest of the runner
		case "go_memstats_gc_pause_total_ns":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.GCPauseTotalNS = uint64(n)
		case "process_cpu_seconds_total":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.CPUSeconds = n
		case "benchmark_bytes_total":
			n, _ := strconv.ParseFloat(valueStr, 64)
			pp.BytesTotal = n
		}
	}
	return pp, true
}

// splitMetricLine splits a line like `go_goroutines 312` or
// `go_memstats_heap_inuse_bytes 1.04857e+08`. Labels (curly-brace forms)
// are intentionally NOT supported — the curated subset uses unlabeled
// metrics only.
func splitMetricLine(line string) (name, value string, ok bool) {
	// Skip lines with labels: `metric{label="x"} value`.
	if strings.ContainsRune(line, '{') {
		return "", "", false
	}
	i := strings.IndexRune(line, ' ')
	if i <= 0 || i == len(line)-1 {
		return "", "", false
	}
	return line[:i], strings.TrimSpace(line[i+1:]), true
}

// parseSnapshots splits a /tmp/prom-N.txt dump on ###timestamp= markers.
// Anything before the first marker is ignored; an empty trailing frame
// (scraper killed mid-write) is kept with Body == "".
func parseSnapshots(r io.Reader) []promSnapshot {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var snaps []promSnapshot
	var current *promSnapshot
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "###timestamp=") {
			if current != nil {
				snaps = append(snaps, *current)
			}
			ts, _ := strconv.ParseInt(strings.TrimPrefix(line, "###timestamp="), 10, 64)
			current = &promSnapshot{UnixTime: ts}
			continue
		}
		if current == nil {
			continue // ignore noise before first marker
		}
		if line == "###scrape_error" {
			current.Errored = true
			continue
		}
		current.Body += line + "\n"
	}
	if current != nil {
		snaps = append(snaps, *current)
	}
	return snaps
}

// ParsePromStream consumes a per-point prom dump and returns the curated
// time series with T reindexed to seconds since the first successful
// snapshot.
func ParsePromStream(r io.Reader) ([]PromPoint, error) {
	snaps := parseSnapshots(r)
	var pts []PromPoint
	var t0 int64
	for _, s := range snaps {
		pp, ok := extractPromPoint(s)
		if !ok {
			continue
		}
		if len(pts) == 0 {
			t0 = s.UnixTime
		}
		pp.T = int(s.UnixTime - t0)
		pts = append(pts, pp)
	}
	return pts, nil
}
