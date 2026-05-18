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
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFileLineRef(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		file    string
		line    int
		wantErr string
	}{
		{
			name:  "simple",
			input: "foo.txt:42",
			file:  "foo.txt",
			line:  42,
		},
		{
			name:  "path with directories",
			input: ".integration/20260331/mssqlserver.txt:870",
			file:  ".integration/20260331/mssqlserver.txt",
			line:  870,
		},
		{
			name:    "no colon",
			input:   "foo.txt",
			wantErr: "expected file:line format",
		},
		{
			name:    "non-numeric line",
			input:   "foo.txt:abc",
			wantErr: "invalid line number",
		},
		{
			name:    "zero line",
			input:   "foo.txt:0",
			wantErr: "line number must be >= 1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			file, line, err := parseFileLineRef(tc.input)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.file, file)
			assert.Equal(t, tc.line, line)
		})
	}
}

func TestMatchesTest(t *testing.T) {
	tests := []struct {
		eventTest string
		target    string
		want      bool
	}{
		{"TestFoo", "TestFoo", true},
		{"TestFoo/sub1", "TestFoo", true},
		{"TestFoo/sub1/deep", "TestFoo", true},
		{"TestFooBar", "TestFoo", false},
		{"TestBar", "TestFoo", false},
		{"", "TestFoo", false},
	}
	for _, tc := range tests {
		t.Run(tc.eventTest+"_"+tc.target, func(t *testing.T) {
			assert.Equal(t, tc.want, matchesTest(tc.eventTest, tc.target))
		})
	}
}

func TestShowTestOutput(t *testing.T) {
	tests := []struct {
		name       string
		file       string
		line       int
		wantOutput string
		wantErr    string
	}{
		{
			name:       "fail with subtest output",
			file:       "testdata/fail.txt",
			line:       8, // fail of parent TestIntegrationRedisCache
			wantOutput: "    redis_test.go:45: expected key to be deleted\n",
		},
		{
			name:       "fail subtest directly",
			file:       "testdata/fail.txt",
			line:       7, // fail of TestIntegrationRedisCache/can_delete_keys
			wantOutput: "    redis_test.go:45: expected key to be deleted\n",
		},
		{
			name:       "mixed file parent test fail",
			file:       "testdata/mixed.txt",
			line:       8, // fail of TestIntegrationNatsJetStream
			wantOutput: "    nats_test.go:89: timed out waiting for message\n",
		},
		{
			name:       "mixed file subtest fail",
			file:       "testdata/mixed.txt",
			line:       7, // fail of TestIntegrationNatsJetStream/can_consume
			wantOutput: "    nats_test.go:89: timed out waiting for message\n",
		},
		{
			name:       "appended file uses correct run",
			file:       "testdata/appended.txt",
			line:       4, // fail of TestIntegrationFoo in first run
			wantOutput: "    integration_test.go:42: some error\n",
		},
		{
			name:       "appended file second run has no output",
			file:       "testdata/appended.txt",
			line:       8, // pass of TestIntegrationFoo in second run (no output events)
			wantOutput: "",
		},
		{
			name:    "line out of range",
			file:    "testdata/fail.txt",
			line:    999,
			wantErr: "requested line 999",
		},
		{
			name:    "package-level event",
			file:    "testdata/fail.txt",
			line:    9, // package fail, no Test field
			wantErr: "no test name",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := showTestOutput(&buf, tc.file, tc.line)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantOutput, buf.String())
		})
	}
}

func TestShowTestOutput_SkippedTest(t *testing.T) {
	var buf bytes.Buffer
	err := showTestOutput(&buf, "testdata/mixed.txt", 12) // skip of TestIntegrationNatsKV
	require.NoError(t, err)

	output := buf.String()
	assert.True(t, strings.Contains(output, "--- SKIP: TestIntegrationNatsKV"))
	assert.True(t, strings.Contains(output, "requires NATS 2.10+"))
}
