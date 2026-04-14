// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// TestResult represents the outcome of a single test.
type TestResult int

const (
	ResultUnknown TestResult = iota
	ResultPass
	ResultFail
	ResultSkip
)

func (r TestResult) String() string {
	switch r {
	case ResultPass:
		return "pass"
	case ResultFail:
		return "fail"
	case ResultSkip:
		return "skip"
	default:
		return "unknown"
	}
}

// CacheEntry holds the cached result for a single test.
type CacheEntry struct {
	TestName   string
	Result     TestResult
	Duration   time.Duration
	FailLine   int
	SkipReason string
}

func (e CacheEntry) IsSubtest() bool {
	return isSubtest(e.TestName)
}

// compactCacheEntries collapses consecutive subtests of the same parent into a
// single parent entry when all subtests passed. Groups with any non-pass result
// are kept as-is so individual failures/skips remain visible.
func compactCacheEntries(entries []CacheEntry) []CacheEntry {
	var out []CacheEntry
	i := 0
	for i < len(entries) {
		e := entries[i]
		if !e.IsSubtest() {
			out = append(out, e)
			i++
			continue
		}

		parent := parentTest(e.TestName)
		j := i
		allPass := true
		for j < len(entries) && entries[j].IsSubtest() && parentTest(entries[j].TestName) == parent {
			if entries[j].Result != ResultPass {
				allPass = false
			}
			j++
		}

		if allPass {
			out = append(out, CacheEntry{
				TestName: parent,
				Result:   ResultPass,
			})
		} else {
			out = append(out, entries[i:j]...)
		}
		i = j
	}
	return out
}

// PackageCache holds cached results for an entire package.
type PackageCache struct {
	Package  string
	Tests    []CacheEntry
	Complete bool
}

// Overall computes the package-level result:
//   - ResultFail if any test failed
//   - ResultPass if the run completed and all tests passed or were skipped
//   - ResultUnknown otherwise (incomplete run, no results)
func (pc PackageCache) Overall() TestResult {
	for _, t := range pc.Tests {
		if t.Result == ResultFail {
			return ResultFail
		}
	}
	if pc.Complete {
		return ResultPass
	}
	return ResultUnknown
}

// checkCache parses a previous JSON-lines output file and returns per-test
// results. Returns a zero PackageCache (Overall() == ResultUnknown) if the file
// doesn't exist or can't be parsed.
func checkCache(outFile string) PackageCache {
	f, err := os.Open(outFile)
	if err != nil {
		return PackageCache{}
	}
	defer f.Close()

	var pc PackageCache
	hasRun := false

	lastAction := parseEvents(f, 0, EventCallbacks{
		OnEvent: func(pe ParsedEvent) {
			if pc.Package == "" && pe.Package != "" {
				pc.Package = pe.Package
			}
			switch pe.Action {
			case ActionRun:
				hasRun = true
			case ActionPass:
				if pe.ParentHasSubs {
					return
				}
				pc.Tests = append(pc.Tests, CacheEntry{
					TestName: pe.Test,
					Result:   ResultPass,
					Duration: time.Duration(pe.Elapsed * float64(time.Second)),
				})
			case ActionFail:
				if pe.ParentHasSubs {
					return
				}
				pc.Tests = append(pc.Tests, CacheEntry{
					TestName: pe.Test,
					Result:   ResultFail,
					Duration: time.Duration(pe.Elapsed * float64(time.Second)),
					FailLine: pe.LineNum,
				})
			case ActionSkip:
				pc.Tests = append(pc.Tests, CacheEntry{
					TestName:   pe.Test,
					Result:     ResultSkip,
					Duration:   time.Duration(pe.Elapsed * float64(time.Second)),
					SkipReason: pe.SkipReason,
				})
			}
		},
	})

	if !hasRun && len(pc.Tests) == 0 {
		return PackageCache{}
	}

	pc.Complete = lastAction == ActionEndRun

	return pc
}

// completedTestSkipRegex builds a -skip regex matching tests that already
// passed or were skipped, so go test only runs remaining/failed tests.
func completedTestSkipRegex(tests []CacheEntry) string {
	var names []string
	for _, t := range tests {
		if t.Result == ResultPass || t.Result == ResultSkip {
			names = append(names, "^"+regexp.QuoteMeta(t.TestName)+"$")
		}
	}
	if len(names) == 0 {
		return ""
	}
	return strings.Join(names, "|")
}

// resolveRunDir always creates a new timestamped directory under .integration/.
func resolveRunDir() string {
	ts := time.Now().Format("20060102150405")
	return filepath.Join(".integration", ts)
}

// findLatestRunDir returns the latest timestamped directory under baseDir.
func findLatestRunDir(baseDir string) string {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return ""
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && hasResultFiles(filepath.Join(baseDir, e.Name())) {
			dirs = append(dirs, e.Name())
		}
	}
	if len(dirs) == 0 {
		return ""
	}
	sort.Strings(dirs)
	return filepath.Join(baseDir, dirs[len(dirs)-1])
}

func hasResultFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.Type().IsRegular() && strings.HasSuffix(e.Name(), ".txt") {
			return true
		}
	}
	return false
}
