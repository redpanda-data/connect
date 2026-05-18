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
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExtractSkipReason(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name:   "space-indented with file prefix",
			input:  "    integration_test.go:33: CON-376: Splunk image is x86-only",
			expect: "CON-376: Splunk image is x86-only",
		},
		{
			name:   "tab-indented with file prefix",
			input:  "\tintegration_test.go:33: CON-376: Splunk image is x86-only",
			expect: "CON-376: Splunk image is x86-only",
		},
		{
			name:   "indented without file prefix",
			input:  "    some reason without file reference",
			expect: "some reason without file reference",
		},
		{
			name:   "non-indented line",
			input:  "PASS",
			expect: "",
		},
		{
			name:   "empty string",
			input:  "",
			expect: "",
		},
		{
			name:   "RUN line (not indented)",
			input:  "=== RUN   TestFoo",
			expect: "",
		},
		{
			name:   "whitespace-only",
			input:  "    ",
			expect: "",
		},
		{
			name:   "JSON output field with trailing newline",
			input:  "    integration_test.go:33: CON-376: Splunk image is x86-only\n",
			expect: "CON-376: Splunk image is x86-only",
		},
		{
			name:   "tab-indented JSON output with trailing newline",
			input:  "\tintegration_test.go:33: CON-376: Splunk image is x86-only\n",
			expect: "CON-376: Splunk image is x86-only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, extractSkipReason(tt.input))
		})
	}
}

func TestFmtElapsed(t *testing.T) {
	tests := []struct {
		name   string
		secs   float64
		expect string
	}{
		{"zero", 0, "0.00s"},
		{"sub-second", 0.05, "0.05s"},
		{"seconds", 1.23, "1.23s"},
		{"minutes", 65.4, "65.40s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, fmtElapsed(tt.secs))
		})
	}
}

func TestWriteEvent(t *testing.T) {
	var buf bytes.Buffer
	ev := TestEvent{Action: ActionPass, Test: "TestFoo", Elapsed: 1.5}
	writeEvent(&buf, ev)
	assert.Contains(t, buf.String(), `"Action":"pass"`)
	assert.Contains(t, buf.String(), `"Test":"TestFoo"`)
	assert.Contains(t, buf.String(), `"Elapsed":1.5`)
	assert.Equal(t, byte('\n'), buf.String()[len(buf.String())-1], "should end with newline")
}

func TestFmtDuration(t *testing.T) {
	tests := []struct {
		name   string
		d      time.Duration
		expect string
	}{
		{"zero", 0, ""},
		{"seconds", 5200 * time.Millisecond, " (5.2s)"},
		{"sub-second", 200 * time.Millisecond, " (0.2s)"},
		{"minutes", 83400 * time.Millisecond, " (1m23.4s)"},
		{"exact minute", 60 * time.Second, " (1m0.0s)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, fmtDuration(tt.d))
		})
	}
}

func TestPkgShort(t *testing.T) {
	tests := []struct {
		pkg    string
		expect string
	}{
		{"./internal/impl/kafka", "kafka"},
		{"./internal/impl/aws/s3", "aws/s3"},
		{"./internal/impl/gcp/enterprise/changestreams", "gcp/enterprise/changestreams"},
		{"./other/path", "./other/path"},
	}

	for _, tt := range tests {
		t.Run(tt.expect, func(t *testing.T) {
			assert.Equal(t, tt.expect, pkgShort(tt.pkg))
		})
	}
}

func TestPkgFilename(t *testing.T) {
	tests := []struct {
		pkg    string
		expect string
	}{
		{"./internal/impl/kafka", "kafka.txt"},
		{"./internal/impl/aws/s3", "aws-s3.txt"},
		{"./internal/impl/snowflake/streaming", "snowflake-streaming.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.expect, func(t *testing.T) {
			assert.Equal(t, tt.expect, pkgFilename(tt.pkg))
		})
	}
}

func TestFilterPackages(t *testing.T) {
	pkgs := []TestPackage{
		{Path: "./internal/impl/kafka"},
		{Path: "./internal/impl/kafka/enterprise"},
		{Path: "./internal/impl/redis"},
		{Path: "./internal/impl/aws/s3"},
		{Path: "./internal/impl/aws/sqs"},
	}

	tests := []struct {
		name    string
		filters []string
		expect  []TestPackage
	}{
		{
			name:    "no filters returns all",
			filters: nil,
			expect:  pkgs,
		},
		{
			name:    "single match",
			filters: []string{"redis"},
			expect:  []TestPackage{{Path: "./internal/impl/redis"}},
		},
		{
			name:    "prefix matches multiple",
			filters: []string{"kafka"},
			expect:  []TestPackage{{Path: "./internal/impl/kafka"}, {Path: "./internal/impl/kafka/enterprise"}},
		},
		{
			name:    "multiple filters",
			filters: []string{"redis", "s3"},
			expect:  []TestPackage{{Path: "./internal/impl/redis"}, {Path: "./internal/impl/aws/s3"}},
		},
		{
			name:    "no matches",
			filters: []string{"nonexistent"},
			expect:  nil,
		},
		{
			name:    "aws matches multiple",
			filters: []string{"aws"},
			expect:  []TestPackage{{Path: "./internal/impl/aws/s3"}, {Path: "./internal/impl/aws/sqs"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, filterPackages(pkgs, tt.filters))
		})
	}
}

func TestIsSubtest(t *testing.T) {
	assert.False(t, isSubtest("TestIntegrationFoo"))
	assert.True(t, isSubtest("TestIntegrationFoo/SubBar"))
}

func TestParentTest(t *testing.T) {
	assert.Equal(t, "TestIntegrationFoo", parentTest("TestIntegrationFoo"))
	assert.Equal(t, "TestIntegrationFoo", parentTest("TestIntegrationFoo/SubBar"))
}
