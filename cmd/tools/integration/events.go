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
	"io"
)

// ParsedEvent is a test event enriched with parser-derived context.
type ParsedEvent struct {
	TestEvent
	LineNum       int
	IsSubtest     bool
	ParentHasSubs bool
	SkipReason    string
}

// EventCallbacks receives parsed test events. All fields are optional.
type EventCallbacks struct {
	// OnRawLine is called for every line before JSON parsing.
	// Receives the raw bytes and the 1-based line number.
	OnRawLine func(line []byte, lineNum int)

	// OnEvent is called for each test-level event (run/pass/fail/skip).
	// Package-level events (Test=="") are not delivered here.
	OnEvent func(ParsedEvent)

	// OnPackageDone is called for package-level pass/fail with elapsed seconds.
	OnPackageDone func(elapsed float64)
}

// parseEvents scans JSON-lines from r, tracking subtest relationships and skip
// reasons. It calls the provided callbacks as events are parsed.
// Returns the last Action seen (useful for detecting end-run markers).
func parseEvents(r io.Reader, startLine int, cb EventCallbacks) Action {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	hasSubtests := make(map[string]struct{})
	lastSkipOutput := make(map[string]string)
	lineNum := startLine

	var lastAction Action

	for scanner.Scan() {
		raw := scanner.Bytes()
		lineNum++

		if cb.OnRawLine != nil {
			cb.OnRawLine(raw, lineNum)
		}

		var ev TestEvent
		if json.Unmarshal(raw, &ev) != nil {
			continue
		}

		lastAction = ev.Action

		// Package-level events.
		if ev.Test == "" {
			if cb.OnPackageDone != nil && (ev.Action == ActionPass || ev.Action == ActionFail) {
				cb.OnPackageDone(ev.Elapsed)
			}
			continue
		}

		sub := isSubtest(ev.Test)
		if sub {
			hasSubtests[parentTest(ev.Test)] = struct{}{}
		}

		switch ev.Action {
		case ActionOutput:
			if len(ev.Output) > 0 && (ev.Output[0] == ' ' || ev.Output[0] == '\t') {
				lastSkipOutput[ev.Test] = ev.Output
			}

		case ActionRun, ActionPass, ActionFail, ActionSkip:
			if cb.OnEvent == nil {
				continue
			}
			_, parentHasSubs := hasSubtests[ev.Test]
			pe := ParsedEvent{
				TestEvent:     ev,
				LineNum:       lineNum,
				IsSubtest:     sub,
				ParentHasSubs: !sub && parentHasSubs,
			}
			if ev.Action == ActionSkip {
				pe.SkipReason = extractSkipReason(lastSkipOutput[ev.Test])
			}
			cb.OnEvent(pe)
		}
	}

	return lastAction
}
