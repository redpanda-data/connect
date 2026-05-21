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
