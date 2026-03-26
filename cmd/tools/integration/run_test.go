// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLine(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		lineNum int
		pending *pendingSkip
		expect  lineEvent
	}{
		{
			name:    "RUN line",
			line:    "=== RUN   TestIntegrationFoo",
			lineNum: 5,
			expect:  lineEvent{kind: eventRun, testName: "TestIntegrationFoo", lineNum: 5},
		},
		{
			name:    "PASS line",
			line:    "--- PASS: TestIntegrationFoo (1.23s)",
			lineNum: 10,
			expect:  lineEvent{kind: eventPass, testName: "TestIntegrationFoo", elapsed: "1.23s", lineNum: 10},
		},
		{
			name:    "FAIL line",
			line:    "--- FAIL: TestIntegrationBar (0.50s)",
			lineNum: 15,
			expect:  lineEvent{kind: eventFail, testName: "TestIntegrationBar", elapsed: "0.50s", lineNum: 15},
		},
		{
			name:    "SKIP line",
			line:    "--- SKIP: TestIntegrationBaz (0.00s)",
			lineNum: 20,
			expect:  lineEvent{kind: eventSkip, testName: "TestIntegrationBaz", elapsed: "0.00s", lineNum: 20},
		},
		{
			name:    "other line",
			line:    "some log output",
			lineNum: 3,
			expect:  lineEvent{kind: eventOther, lineNum: 3},
		},
		{
			name:    "subtest RUN",
			line:    "=== RUN   TestIntegrationFoo/subtest",
			lineNum: 6,
			expect:  lineEvent{kind: eventRun, testName: "TestIntegrationFoo/subtest", lineNum: 6, subtest: true},
		},
		{
			name:    "subtest PASS (indented)",
			line:    "    --- PASS: TestIntegrationFoo/subtest (0.01s)",
			lineNum: 7,
			expect:  lineEvent{kind: eventPass, testName: "TestIntegrationFoo/subtest", elapsed: "0.01s", lineNum: 7, subtest: true},
		},
		{
			name:    "subtest FAIL (indented)",
			line:    "    --- FAIL: TestIntegrationFoo/subtest (0.50s)",
			lineNum: 8,
			expect:  lineEvent{kind: eventFail, testName: "TestIntegrationFoo/subtest", elapsed: "0.50s", lineNum: 8, subtest: true},
		},
		{
			name:    "subtest SKIP (indented)",
			line:    "    --- SKIP: TestIntegrationFoo/subtest (0.00s)",
			lineNum: 9,
			expect:  lineEvent{kind: eventSkip, testName: "TestIntegrationFoo/subtest", elapsed: "0.00s", lineNum: 9, subtest: true},
		},
		{
			name:    "pending skip with reason",
			line:    "    integration_test.go:33: CON-376: image is x86-only",
			lineNum: 21,
			pending: &pendingSkip{testName: "TestIntegrationBaz", elapsed: "0.00s"},
			expect: lineEvent{
				kind:     eventSkipReason,
				testName: "TestIntegrationBaz",
				elapsed:  "0.00s",
				reason:   "CON-376: image is x86-only",
				lineNum:  21,
			},
		},
		{
			name:    "pending skip without reason (next line is not indented)",
			line:    "PASS",
			lineNum: 22,
			pending: &pendingSkip{testName: "TestIntegrationBaz", elapsed: "0.00s"},
			expect: lineEvent{
				kind:     eventSkipReason,
				testName: "TestIntegrationBaz",
				elapsed:  "0.00s",
				reason:   "",
				lineNum:  22,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseLine(tt.line, tt.lineNum, tt.pending)
			assert.Equal(t, tt.expect, got)
		})
	}
}
