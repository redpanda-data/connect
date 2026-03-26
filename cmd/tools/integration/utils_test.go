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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExtractSkipReason(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		expect string
	}{
		{
			name:   "space-indented with file prefix",
			line:   "    integration_test.go:33: CON-376: Splunk image is x86-only",
			expect: "CON-376: Splunk image is x86-only",
		},
		{
			name:   "tab-indented with file prefix",
			line:   "\tintegration_test.go:33: CON-376: Splunk image is x86-only",
			expect: "CON-376: Splunk image is x86-only",
		},
		{
			name:   "indented without file prefix",
			line:   "    some reason without file reference",
			expect: "some reason without file reference",
		},
		{
			name:   "non-indented line",
			line:   "PASS",
			expect: "",
		},
		{
			name:   "empty line",
			line:   "",
			expect: "",
		},
		{
			name:   "RUN line (not a reason)",
			line:   "=== RUN   TestFoo",
			expect: "",
		},
		{
			name:   "whitespace-only line",
			line:   "    ",
			expect: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, extractSkipReason(tt.line))
		})
	}
}

func TestExtractTime(t *testing.T) {
	tests := []struct {
		name    string
		content string
		expect  time.Duration
	}{
		{
			name:    "ok line",
			content: "ok  \tgithub.com/foo/bar\t2.706s\n",
			expect:  2706 * time.Millisecond,
		},
		{
			name:    "FAIL line",
			content: "FAIL\tgithub.com/foo/bar\t1.234s\n",
			expect:  1234 * time.Millisecond,
		},
		{
			name:    "no timing",
			content: "=== RUN TestFoo\n--- PASS: TestFoo (0.05s)\n",
			expect:  0,
		},
		{
			name:    "ok preferred over FAIL",
			content: "ok  \tgithub.com/foo/bar\t3.000s\nFAIL\tgithub.com/foo/bar\t1.000s\n",
			expect:  3000 * time.Millisecond,
		},
		{
			name:    "empty",
			content: "",
			expect:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, extractTime(tt.content))
		})
	}
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
