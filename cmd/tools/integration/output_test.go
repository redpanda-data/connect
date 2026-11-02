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
	"os"
	"path/filepath"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestOutput(t *testing.T) (*Output, *bytes.Buffer, string) {
	t.Helper()
	t.Setenv("CI", "")
	t.Setenv("FORCE_COLOR", "")
	noColor := color.NoColor
	color.NoColor = true
	t.Cleanup(func() { color.NoColor = noColor })

	var term bytes.Buffer
	indexPath := filepath.Join(t.TempDir(), "index.md")
	out := NewOutput(&term, indexPath)
	out.Header("Integration Tests (2 packages)")
	return out, &term, indexPath
}

func TestOutputBufferedPackagesDoNotInterleave(t *testing.T) {
	out, term, indexPath := newTestOutput(t)

	kafka := out.Package("kafka", 1, 2, "kafka", true)
	redis := out.Package("redis", 2, 2, "redis", true)
	kafka.TestPass("TestKafka", "1.00s", false)
	redis.TestPass("TestRedis", "2.00s", false)
	redis.Result("redis", ResultPass, 0)
	kafka.Result("kafka", ResultPass, 0)

	assert.Equal(t, "Integration Tests (2 packages)\n"+
		"[2/2] redis\n"+
		"  PASS TestRedis (2.00s)\n"+
		"✓ redis\n"+
		"\n"+
		"[1/2] kafka\n"+
		"  PASS TestKafka (1.00s)\n"+
		"✓ kafka\n"+
		"\n", term.String())

	index, err := os.ReadFile(indexPath)
	require.NoError(t, err)
	assert.Equal(t, "# Integration Tests (2 packages)\n\n"+
		"## redis\n\n"+
		"- PASS TestRedis (2.00s)\n\n"+
		"## kafka\n\n"+
		"- PASS TestKafka (1.00s)\n\n", string(index))
}

func TestOutputStreamedPackageWritesAtOnce(t *testing.T) {
	out, term, _ := newTestOutput(t)

	kafka := out.Package("kafka", 1, 1, "kafka", false)
	kafka.TestRun("TestKafka")

	assert.Equal(t, "Integration Tests (2 packages)\n"+
		"[1/1] kafka\n"+
		"  RUN  TestKafka\n", term.String())
}
