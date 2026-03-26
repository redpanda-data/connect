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
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Regex patterns for parsing go test -v output.
// These match both top-level tests and subtests (Parent/Sub).
var (
	reRun  = regexp.MustCompile(`^=== RUN\s+(\S+)$`)
	rePass = regexp.MustCompile(`^\s*--- PASS: (\S+) \(([^)]+)\)$`)
	reFail = regexp.MustCompile(`^\s*--- FAIL: (\S+) \(([^)]+)\)$`)
	reSkip = regexp.MustCompile(`^\s*--- SKIP: (\S+) \(([^)]+)\)$`)

	reOKTime   = regexp.MustCompile(`^ok\s.*?(\d+\.\d+s)`)
	reFAILTime = regexp.MustCompile(`^FAIL\s.*?(\d+\.\d+s)`)

	reSkipReason = regexp.MustCompile(`^\s+\w+\.go:\d+: (.+)$`)
)

func isSubtest(name string) bool {
	return strings.Contains(name, "/")
}

func parentTest(name string) string {
	if before, _, found := strings.Cut(name, "/"); found {
		return before
	}
	return name
}

// extractSkipReason extracts the skip reason from an indented line following
// a --- SKIP line. Strips the "file.go:NN: " prefix if present.
func extractSkipReason(line string) string {
	if m := reSkipReason.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	// Only consider indented lines as potential skip reasons.
	if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
		if reason := strings.TrimSpace(line); reason != "" {
			return reason
		}
	}
	return ""
}

// extractTime pulls the timing from the ok/FAIL summary line.
// Prefers the ok line over FAIL when both are present.
func extractTime(content string) time.Duration {
	for line := range strings.SplitSeq(content, "\n") {
		if m := reOKTime.FindStringSubmatch(line); m != nil {
			if d, err := time.ParseDuration(m[1]); err == nil {
				return d
			}
		}
		if m := reFAILTime.FindStringSubmatch(line); m != nil {
			if d, err := time.ParseDuration(m[1]); err == nil {
				return d
			}
		}
	}
	return 0
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
