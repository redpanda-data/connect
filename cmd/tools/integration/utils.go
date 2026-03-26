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
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"
)

// Action represents a test event action from `go test -json` output.
type Action string

const (
	ActionStart    Action = "start"     // package starts
	ActionRun      Action = "run"       // test starts
	ActionOutput   Action = "output"    // test prints output
	ActionPass     Action = "pass"      // test or package passes
	ActionFail     Action = "fail"      // test or package fails
	ActionSkip     Action = "skip"      // test is skipped
	ActionStartRun Action = "start-run" // custom: marks the beginning of a runner invocation
	ActionEndRun   Action = "end-run"   // custom: marks the end of a runner invocation
)

// TestEvent represents a single JSON event from `go test -json` output.
// Also used for custom run markers (ActionStartRun, ActionEndRun).
type TestEvent struct {
	Time    time.Time `json:"Time,omitzero"`
	Action  Action    `json:"Action"`
	Package string    `json:"Package,omitempty"`
	Test    string    `json:"Test,omitempty"`
	Output  string    `json:"Output,omitempty"`
	Elapsed float64   `json:"Elapsed,omitempty"`
}

// writeEvent marshals a TestEvent as a single JSON line.
func writeEvent(w io.Writer, ev TestEvent) {
	data, _ := json.Marshal(ev)
	fmt.Fprintln(w, string(data))
}

func isSubtest(name string) bool {
	return strings.Contains(name, "/")
}

func parentTest(name string) string {
	if before, _, found := strings.Cut(name, "/"); found {
		return before
	}
	return name
}

// extractSkipReason extracts the skip reason from a test output string.
// Strips the "file.go:NN: " prefix if present. Only indented lines
// (starting with space or tab) are treated as skip reasons.
// Input is typically the Output field of a JSON test event, e.g.:
//
//	"    foo_test.go:42: reason here\n"
func extractSkipReason(output string) string {
	s := strings.TrimRight(output, "\n")
	if len(s) == 0 {
		return ""
	}
	if s[0] != ' ' && s[0] != '\t' {
		return ""
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Strip "file.go:NN: " prefix if present.
	if idx := strings.Index(s, ": "); idx > 0 {
		prefix := s[:idx]
		if colonIdx := strings.LastIndex(prefix, ":"); colonIdx > 0 {
			if strings.HasSuffix(prefix[:colonIdx], ".go") {
				return s[idx+2:]
			}
		}
	}
	return s
}

// fmtElapsed formats a float64 seconds value as "1.23s".
func fmtElapsed(secs float64) string {
	return fmt.Sprintf("%.2fs", secs)
}

// fmtDuration formats a duration as (1m23.4s) or (5.2s), or empty if zero.
func fmtDuration(d time.Duration) string {
	if d == 0 {
		return ""
	}
	if d >= time.Minute {
		mins := int(d.Minutes())
		secs := d.Seconds() - float64(mins)*60
		return fmt.Sprintf(" (%.0dm%.1fs)", mins, secs)
	}
	return fmt.Sprintf(" (%.1fs)", d.Seconds())
}

func pkgShort(pkg string) string {
	return strings.TrimPrefix(pkg, "./internal/impl/")
}

func pkgFilename(pkg string) string {
	return strings.ReplaceAll(pkgShort(pkg), "/", "-") + ".txt"
}

// countFileLines counts newlines in a file without loading it all into memory.
// Returns 0 if the file doesn't exist.
func countFileLines(path string) (lines int, endsWithNewline bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, true, nil
		}
		return 0, false, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, false, err
	}
	if info.Size() == 0 {
		return 0, true, nil
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		lines++
	}

	// Check last byte directly.
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, info.Size()-1); err != nil {
		return lines, true, nil
	}
	return lines, buf[0] == '\n', nil
}

func filterPackages(packages []TestPackage, filters []string) []TestPackage {
	if len(filters) == 0 {
		return packages
	}
	var filtered []TestPackage
	for _, pkg := range packages {
		for _, f := range filters {
			if strings.Contains(pkg.Path, f) {
				filtered = append(filtered, pkg)
				break
			}
		}
	}
	return filtered
}
