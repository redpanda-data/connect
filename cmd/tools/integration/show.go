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
	"os"
	"strconv"
	"strings"
)

func cmdShow(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: integration show <file>:<line>")
	}

	file, line, err := parseFileLineRef(args[0])
	if err != nil {
		return err
	}

	return showTestOutput(os.Stdout, file, line)
}

// parseFileLineRef splits "path/to/file.txt:42" into (path, 42, nil).
func parseFileLineRef(ref string) (string, int, error) {
	idx := strings.LastIndex(ref, ":")
	if idx < 0 {
		return "", 0, fmt.Errorf("expected file:line format, got %q", ref)
	}
	line, err := strconv.Atoi(ref[idx+1:])
	if err != nil {
		return "", 0, fmt.Errorf("invalid line number in %q: %w", ref, err)
	}
	if line < 1 {
		return "", 0, fmt.Errorf("line number must be >= 1, got %d", line)
	}
	return ref[:idx], line, nil
}

// showTestOutput reads the file up to the target line, finds the test name,
// then prints all output events for that test from the enclosing run.
func showTestOutput(w io.Writer, file string, targetLine int) error {
	lines, err := readLines(file, targetLine)
	if err != nil {
		return err
	}

	// Extract test name from target line.
	targetIdx := targetLine - 1
	var target TestEvent
	if err := json.Unmarshal([]byte(lines[targetIdx]), &target); err != nil {
		return fmt.Errorf("line %d is not valid JSON: %w", targetLine, err)
	}
	testName := target.Test
	if testName == "" {
		return fmt.Errorf("line %d has no test name (package-level event)", targetLine)
	}

	// Scan backwards to find the start of this run.
	runStart := 0
	for i := targetIdx - 1; i >= 0; i-- {
		var ev TestEvent
		if json.Unmarshal([]byte(lines[i]), &ev) == nil && ev.Action == ActionStartRun {
			runStart = i
			break
		}
	}

	// Collect and print output events for the test (and its subtests).
	for i := runStart; i <= targetIdx; i++ {
		var ev TestEvent
		if json.Unmarshal([]byte(lines[i]), &ev) != nil {
			continue
		}
		if ev.Action != ActionOutput {
			continue
		}
		if !matchesTest(ev.Test, testName) {
			continue
		}
		fmt.Fprint(w, ev.Output)
	}

	return nil
}

// matchesTest returns true if eventTest is the target test or one of its subtests.
func matchesTest(eventTest, target string) bool {
	if eventTest == target {
		return true
	}
	return strings.HasPrefix(eventTest, target+"/")
}

// readLines reads the first n lines from file and returns them.
func readLines(file string, n int) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	lines := make([]string, 0, n)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) == n {
			return lines, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("file has %d lines, requested line %d", len(lines), n)
}
