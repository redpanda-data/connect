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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckCache(t *testing.T) {
	tests := []struct {
		name     string
		fixture  string
		overall  TestResult
		complete bool
		numTests int
	}{
		{
			name:     "passing package with subtests",
			fixture:  "testdata/pass.txt",
			overall:  ResultPass,
			complete: true,
			numTests: 5, // 5 subtests, parent excluded
		},
		{
			name:     "failing package with subtests",
			fixture:  "testdata/fail.txt",
			overall:  ResultFail,
			complete: true,
			numTests: 2, // 2 subtests (1 pass + 1 fail), parent excluded
		},
		{
			name:     "skipped package (no subtests)",
			fixture:  "testdata/skip.txt",
			overall:  ResultPass,
			complete: true,
			numTests: 1,
		},
		{
			name:     "mixed results with subtests",
			fixture:  "testdata/mixed.txt",
			overall:  ResultFail,
			complete: true,
			numTests: 3, // 2 subtests from NatsJetStream + 1 top-level NatsKV skip
		},
		{
			name:     "incomplete file with subtest",
			fixture:  "testdata/incomplete.txt",
			overall:  ResultUnknown,
			complete: false,
			numTests: 1, // 1 subtest pass
		},
		{
			name:    "nonexistent file (guard value)",
			fixture: "testdata/nonexistent.txt",
			overall: ResultUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := checkCache(tt.fixture)
			assert.Equal(t, tt.overall, pc.Overall())
			assert.Equal(t, tt.complete, pc.Complete)
			assert.Len(t, pc.Tests, tt.numTests)
		})
	}
}

func TestCheckCachePassSubtests(t *testing.T) {
	pc := checkCache("testdata/pass.txt")

	require.Len(t, pc.Tests, 5)

	// All entries are subtests with full Parent/Sub names.
	for _, entry := range pc.Tests {
		assert.True(t, isSubtest(entry.TestName), "expected subtest name, got %s", entry.TestName)
		assert.Equal(t, "TestIntegrationMemcachedCache", parentTest(entry.TestName))
		assert.Equal(t, ResultPass, entry.Result)
	}
}

func TestCheckCacheFailSubtests(t *testing.T) {
	pc := checkCache("testdata/fail.txt")

	require.Len(t, pc.Tests, 2)

	assert.Equal(t, "TestIntegrationRedisCache/can_get_and_set", pc.Tests[0].TestName)
	assert.Equal(t, ResultPass, pc.Tests[0].Result)

	assert.Equal(t, "TestIntegrationRedisCache/can_delete_keys", pc.Tests[1].TestName)
	assert.Equal(t, ResultFail, pc.Tests[1].Result)
	assert.Greater(t, pc.Tests[1].FailLine, 0)
}

func TestCheckCacheSkipDetails(t *testing.T) {
	pc := checkCache("testdata/skip.txt")

	require.Len(t, pc.Tests, 1)

	entry := pc.Tests[0]
	assert.Equal(t, "TestIntegrationSplunk", entry.TestName)
	assert.Equal(t, ResultSkip, entry.Result)
	assert.Contains(t, entry.SkipReason, "CON-376")
}

func TestCheckCacheSkipTabIndent(t *testing.T) {
	pc := checkCache("testdata/skip_tab_indent.txt")

	require.Len(t, pc.Tests, 1)

	entry := pc.Tests[0]
	assert.Equal(t, ResultSkip, entry.Result)
	assert.Contains(t, entry.SkipReason, "CON-376")
}

func TestCheckCacheIncompleteSubtest(t *testing.T) {
	pc := checkCache("testdata/incomplete.txt")

	assert.False(t, pc.Complete)
	// The one subtest that completed is captured.
	require.Len(t, pc.Tests, 1)
	assert.Equal(t, "TestIntegrationKafka/can_produce", pc.Tests[0].TestName)
	assert.Equal(t, ResultPass, pc.Tests[0].Result)
}

func TestCheckCacheMixedSubtests(t *testing.T) {
	pc := checkCache("testdata/mixed.txt")

	require.Len(t, pc.Tests, 3)

	// NatsJetStream subtests (parent excluded).
	assert.Equal(t, "TestIntegrationNatsJetStream/can_produce", pc.Tests[0].TestName)
	assert.Equal(t, ResultPass, pc.Tests[0].Result)

	assert.Equal(t, "TestIntegrationNatsJetStream/can_consume", pc.Tests[1].TestName)
	assert.Equal(t, ResultFail, pc.Tests[1].Result)

	// NatsKV has no subtests, kept as top-level.
	assert.Equal(t, "TestIntegrationNatsKV", pc.Tests[2].TestName)
	assert.Equal(t, ResultSkip, pc.Tests[2].Result)
}

func TestOverall(t *testing.T) {
	tests := []struct {
		name     string
		tests    []CacheEntry
		complete bool
		expect   TestResult
	}{
		{
			name:     "empty incomplete",
			complete: false,
			expect:   ResultUnknown,
		},
		{
			name:     "empty complete",
			complete: true,
			expect:   ResultPass,
		},
		{
			name:     "all pass complete",
			tests:    []CacheEntry{{Result: ResultPass}, {Result: ResultPass}},
			complete: true,
			expect:   ResultPass,
		},
		{
			name:     "all skip complete",
			tests:    []CacheEntry{{Result: ResultSkip}, {Result: ResultSkip}},
			complete: true,
			expect:   ResultPass,
		},
		{
			name:     "any fail complete",
			tests:    []CacheEntry{{Result: ResultPass}, {Result: ResultFail}},
			complete: true,
			expect:   ResultFail,
		},
		{
			name:     "any fail incomplete",
			tests:    []CacheEntry{{Result: ResultPass}, {Result: ResultFail}},
			complete: false,
			expect:   ResultFail,
		},
		{
			name:     "pass incomplete",
			tests:    []CacheEntry{{Result: ResultPass}},
			complete: false,
			expect:   ResultUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := &PackageCache{Tests: tt.tests, Complete: tt.complete}
			assert.Equal(t, tt.expect, pc.Overall())
		})
	}
}

func TestCompletedTestSkipRegex(t *testing.T) {
	tests := []struct {
		name   string
		tests  []CacheEntry
		expect string
	}{
		{
			name:   "no completed",
			tests:  []CacheEntry{{TestName: "TestFoo", Result: ResultFail}},
			expect: "",
		},
		{
			name:   "single pass",
			tests:  []CacheEntry{{TestName: "TestFoo", Result: ResultPass}},
			expect: "^TestFoo$",
		},
		{
			name:   "subtest pass",
			tests:  []CacheEntry{{TestName: "TestFoo/Sub1", Result: ResultPass}},
			expect: "^TestFoo/Sub1$",
		},
		{
			name: "mixed subtests",
			tests: []CacheEntry{
				{TestName: "TestFoo/Sub1", Result: ResultPass},
				{TestName: "TestFoo/Sub2", Result: ResultFail},
				{TestName: "TestFoo/Sub3", Result: ResultSkip},
			},
			expect: "^TestFoo/Sub1$|^TestFoo/Sub3$",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, completedTestSkipRegex(tt.tests))
		})
	}
}

func TestCompactCacheEntries(t *testing.T) {
	tests := []struct {
		name   string
		input  []CacheEntry
		expect []CacheEntry
	}{
		{
			name:   "empty",
			input:  nil,
			expect: nil,
		},
		{
			name: "top-level only, no subtests",
			input: []CacheEntry{
				{TestName: "TestFoo", Result: ResultPass},
				{TestName: "TestBar", Result: ResultFail, FailLine: 42},
			},
			expect: []CacheEntry{
				{TestName: "TestFoo", Result: ResultPass},
				{TestName: "TestBar", Result: ResultFail, FailLine: 42},
			},
		},
		{
			name: "all subtests pass, collapsed to parent",
			input: []CacheEntry{
				{TestName: "TestFoo/sub1", Result: ResultPass},
				{TestName: "TestFoo/sub2", Result: ResultPass},
				{TestName: "TestFoo/sub3", Result: ResultPass},
			},
			expect: []CacheEntry{
				{TestName: "TestFoo", Result: ResultPass},
			},
		},
		{
			name: "subtests with a failure, kept individually",
			input: []CacheEntry{
				{TestName: "TestFoo/sub1", Result: ResultPass},
				{TestName: "TestFoo/sub2", Result: ResultFail, FailLine: 10},
			},
			expect: []CacheEntry{
				{TestName: "TestFoo/sub1", Result: ResultPass},
				{TestName: "TestFoo/sub2", Result: ResultFail, FailLine: 10},
			},
		},
		{
			name: "subtests with a skip, kept individually",
			input: []CacheEntry{
				{TestName: "TestFoo/sub1", Result: ResultPass},
				{TestName: "TestFoo/sub2", Result: ResultSkip, SkipReason: "no docker"},
			},
			expect: []CacheEntry{
				{TestName: "TestFoo/sub1", Result: ResultPass},
				{TestName: "TestFoo/sub2", Result: ResultSkip, SkipReason: "no docker"},
			},
		},
		{
			name: "mixed: passing group then top-level then failing group",
			input: []CacheEntry{
				{TestName: "TestA/sub1", Result: ResultPass},
				{TestName: "TestA/sub2", Result: ResultPass},
				{TestName: "TestB", Result: ResultSkip},
				{TestName: "TestC/sub1", Result: ResultPass},
				{TestName: "TestC/sub2", Result: ResultFail, FailLine: 55},
			},
			expect: []CacheEntry{
				{TestName: "TestA", Result: ResultPass},
				{TestName: "TestB", Result: ResultSkip},
				{TestName: "TestC/sub1", Result: ResultPass},
				{TestName: "TestC/sub2", Result: ResultFail, FailLine: 55},
			},
		},
		{
			name: "different parents interleaved by top-level",
			input: []CacheEntry{
				{TestName: "TestX/a", Result: ResultPass},
				{TestName: "TestY", Result: ResultPass},
				{TestName: "TestZ/b", Result: ResultPass},
			},
			expect: []CacheEntry{
				{TestName: "TestX", Result: ResultPass},
				{TestName: "TestY", Result: ResultPass},
				{TestName: "TestZ", Result: ResultPass},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compactCacheEntries(tt.input)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestCheckCacheMultipleSections(t *testing.T) {
	pc := checkCache("testdata/appended.txt")

	// Both sections are parsed: first run's FAIL and second run's PASS.
	require.Len(t, pc.Tests, 2)
	assert.Equal(t, "TestIntegrationFoo", pc.Tests[0].TestName)
	assert.Equal(t, ResultFail, pc.Tests[0].Result)
	assert.Equal(t, 4, pc.Tests[0].FailLine)
	assert.Equal(t, "TestIntegrationFoo", pc.Tests[1].TestName)
	assert.Equal(t, ResultPass, pc.Tests[1].Result)
	assert.True(t, pc.Complete)
}

func TestFindLatestRunDir(t *testing.T) {
	base := t.TempDir()

	t.Run("empty directory", func(t *testing.T) {
		assert.Empty(t, findLatestRunDir(base))
	})

	t.Run("returns latest by name sort", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(filepath.Join(base, "20260101000000"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(base, "20260102000000"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(base, "20260101120000"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(base, "20260102000000", "pkg.txt"), []byte("ok"), 0o644))

		got := findLatestRunDir(base)
		assert.Equal(t, filepath.Join(base, "20260102000000"), got)
	})

	t.Run("nonexistent directory", func(t *testing.T) {
		assert.Empty(t, findLatestRunDir("/nonexistent/path"))
	})
}
