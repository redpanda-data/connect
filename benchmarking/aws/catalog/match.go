// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	"fmt"
	"math"
	"slices"
	"sort"
)

// Query describes the workload a sizing request is about.
type Query struct {
	Input            string
	Output           string
	Processors       []string
	Fingerprint      string // "" when the request had no config
	MessageSizeBytes int    // 0 when unknown
	Mode             string
}

// MatchKind classifies how closely an entry matches a query.
type MatchKind string

// Match kinds.
const (
	MatchExact MatchKind = "exact"
	MatchClose MatchKind = "close"
)

// Match is a catalogue entry that matches a query.
type Match struct {
	Entry       Entry
	Kind        MatchKind
	Score       float64
	Differences []string
}

// FindMatches returns the entries matching q, best first. An exact match has
// the same input, output, mode and processor set, the same fingerprint (when
// the query has one) and a message size within 10% of the query's. A close
// match only shares the input and output. Unknown (zero) message sizes never
// produce an exact match and contribute nothing to the score.
func FindMatches(entries []Entry, q Query) []Match {
	var out []Match
	for _, e := range entries {
		if e.Input != q.Input || e.Output != q.Output {
			continue
		}
		out = append(out, scoreMatch(e, q))
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if (a.Kind == MatchExact) != (b.Kind == MatchExact) {
			return a.Kind == MatchExact
		}
		if a.Score != b.Score {
			return a.Score < b.Score
		}
		if !a.Entry.RecordedAt.Equal(b.Entry.RecordedAt) {
			return a.Entry.RecordedAt.After(b.Entry.RecordedAt)
		}
		return a.Entry.ID < b.Entry.ID
	})
	return out
}

func scoreMatch(e Entry, q Query) Match {
	m := Match{Entry: e, Kind: MatchClose}
	sizesKnown := q.MessageSizeBytes > 0 && e.Workload.MessageSizeBytes > 0

	if sizesKnown {
		m.Score += math.Abs(math.Log(float64(e.Workload.MessageSizeBytes) / float64(q.MessageSizeBytes)))
		if e.Workload.MessageSizeBytes != q.MessageSizeBytes {
			m.Differences = append(m.Differences, fmt.Sprintf("message size %d vs %d bytes", e.Workload.MessageSizeBytes, q.MessageSizeBytes))
		}
	}

	extra, missing := setDiff(e.Processors, q.Processors)
	m.Score += 0.5 * float64(len(extra)+len(missing))
	if len(extra) > 0 {
		m.Differences = append(m.Differences, fmt.Sprintf("entry has extra processors %v", extra))
	}
	if len(missing) > 0 {
		m.Differences = append(m.Differences, fmt.Sprintf("entry lacks processors %v", missing))
	}

	modeSame := e.Workload.Mode == q.Mode
	if !modeSame {
		m.Score++
		m.Differences = append(m.Differences, fmt.Sprintf("mode %s vs %s", e.Workload.Mode, q.Mode))
	}

	fpSame := q.Fingerprint == "" || e.Fingerprint == q.Fingerprint
	if !fpSame {
		m.Differences = append(m.Differences, "config fingerprint differs")
	}

	sizeOK := sizesKnown && absInt(e.Workload.MessageSizeBytes-q.MessageSizeBytes)*10 <= q.MessageSizeBytes
	if modeSame && fpSame && sizeOK && len(extra)+len(missing) == 0 {
		m.Kind = MatchExact
	}
	return m
}

// setDiff returns the members of a not in b, and of b not in a.
func setDiff(a, b []string) (onlyA, onlyB []string) {
	for _, s := range a {
		if !slices.Contains(b, s) && !slices.Contains(onlyA, s) {
			onlyA = append(onlyA, s)
		}
	}
	for _, s := range b {
		if !slices.Contains(a, s) && !slices.Contains(onlyB, s) {
			onlyB = append(onlyB, s)
		}
	}
	return onlyA, onlyB
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
