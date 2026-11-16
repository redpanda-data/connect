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
//go:build unix

package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeTask stands in for the task binary. It appends its arguments to
// $FAKE_TASK_CALLS and behaves per task name: bench:run logs two busy samples
// and then idles until interrupted, when it logs its total stats like the
// benchmark processor does at shutdown.
const fakeTask = `#!/bin/sh
echo "$*" >> "$FAKE_TASK_CALLS"
case "$3" in
  "$FAKE_TASK_FAIL")
    echo "boom"
    exit 3
    ;;
  bench:run)
    trap 'echo "INFO total stats: 80 msg/sec, 800 kB/sec"; exit 130' INT
    echo "INFO rolling stats: 0 msg/sec, 0 B/sec"
    echo "INFO rolling stats: 100 msg/sec, 1 MB/sec"
    echo "INFO rolling stats: 300 msg/sec, 3 MB/sec"
    while true; do
      echo "INFO rolling stats: 0 msg/sec, 0 B/sec"
      sleep 0.05
    done
    ;;
  bench:run:finite|bench:run:cdc)
    echo "INFO rolling stats: 50 msg/sec, 500 kB/sec"
    ;;
  bench:run:silent)
    trap 'exit 130' INT
    while true; do sleep 0.05; done
    ;;
esac
`

type fakeRun struct {
	root  string
	suite *Suite
	calls string
}

func newFakeRun(t *testing.T) *fakeRun {
	t.Helper()
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "task"), []byte(fakeTask), 0o755))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	f := &fakeRun{root: t.TempDir(), calls: filepath.Join(bin, "calls")}
	t.Setenv("FAKE_TASK_CALLS", f.calls)
	t.Setenv("FAKE_TASK_FAIL", "none")
	t.Setenv("WIDGET_DSN", "widget://localhost")

	manifest := strings.ReplaceAll(validManifest, "tools: [docker]", "tools: [sh]") +
		"  - name: finite\n    task: bench:run:finite\n  - name: silent\n    task: bench:run:silent\n"
	taskfile := validTaskfile + "  bench:run:finite: {cmd: \"true\"}\n  bench:run:silent: {cmd: \"true\"}\n"
	writeSuite(t, f.root, "internal/impl/widget/bench", manifest, taskfile)
	suites, err := discover(f.root)
	require.NoError(t, err)
	require.Len(t, suites, 1)
	require.Empty(t, suites[0].Problems)
	f.suite = suites[0]
	return f
}

func (f *fakeRun) run(t *testing.T, scenario string, opts runOptions) *Result {
	t.Helper()
	sc, err := f.suite.scenario(scenario)
	require.NoError(t, err)
	if opts.outDir == "" {
		opts.outDir = filepath.Join(f.root, "out")
	}
	if opts.idleSamples == 0 {
		opts.idleSamples = 3
	}
	res, err := runSuite(t.Context(), f.root, f.suite, sc, opts, io.Discard)
	require.NoError(t, err)

	// What is returned is what was written.
	raw, err := os.ReadFile(filepath.Join(f.root, filepath.FromSlash(res.ResultPath)))
	require.NoError(t, err)
	var onDisk Result
	require.NoError(t, json.Unmarshal(raw, &onDisk))
	assert.Equal(t, res.Status, onDisk.Status)
	assert.Equal(t, res.Summary, onDisk.Summary)
	assert.Len(t, onDisk.Samples, len(res.Samples))
	return res
}

// tasks returns the task names the fake was called with, in order, and the
// variables of the last call.
func (f *fakeRun) tasks(t *testing.T) (names []string, vars []string) {
	t.Helper()
	raw, err := os.ReadFile(f.calls)
	require.NoError(t, err)
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		require.GreaterOrEqual(t, len(fields), 3)
		assert.Equal(t, "--dir", fields[0])
		assert.Equal(t, filepath.Join(f.root, "internal/impl/widget/bench"), fields[1])
		names = append(names, fields[2])
		vars = fields[3:]
	}
	return names, vars
}

func TestRunStopsWhenIdle(t *testing.T) {
	f := newFakeRun(t)
	res := f.run(t, "", runOptions{vars: map[string]string{"BATCH": "500"}})

	assert.Equal(t, statusOK, res.Status, res.Error)
	assert.Equal(t, stopIdle, res.StopReason)
	assert.Equal(t, resultSchemaVersion, res.SchemaVersion)
	assert.Equal(t, "widget_cdc", res.Suite)
	assert.Equal(t, "widget_cdc", res.Component)
	assert.Equal(t, "snapshot", res.Scenario)
	assert.Equal(t, map[string]string{"BATCH": "500"}, res.Vars)
	assert.Equal(t, "out/result.json", res.ResultPath)
	assert.Positive(t, res.DurationSeconds)

	assert.Equal(t, 2, res.Summary.Samples)
	assert.InDelta(t, 200, res.Summary.MeanMsgPerSec, 1e-9)
	assert.InDelta(t, 3, res.Summary.PeakMBPerSec, 1e-9)
	// Logged by the suite while it shut down after the interrupt.
	assert.Equal(t, &Total{MsgPerSec: 80, MBPerSec: 0.8}, res.Total)

	names, vars := f.tasks(t)
	assert.Equal(t, []string{taskUp, taskLoad, taskReset, taskRun, taskDown}, names)
	assert.Equal(t, []string{"BATCH=500"}, vars)

	log, err := os.ReadFile(filepath.Join(f.root, "out", "run.log"))
	require.NoError(t, err)
	assert.Contains(t, string(log), "rolling stats: 300 msg/sec")
}

func TestRunScenarioTaskAndVars(t *testing.T) {
	f := newFakeRun(t)
	// The flag overrides the scenario's CORES=4.
	res := f.run(t, "streaming", runOptions{skipSetup: true, vars: map[string]string{"CORES": "8"}})

	assert.Equal(t, statusOK, res.Status, res.Error)
	names, vars := f.tasks(t)
	assert.Equal(t, []string{taskReset, "bench:run:cdc"}, names)
	assert.Equal(t, []string{"CORES=8"}, vars)
}

func TestRunThatExitsByItself(t *testing.T) {
	f := newFakeRun(t)
	res := f.run(t, "finite", runOptions{keep: true})

	assert.Equal(t, statusOK, res.Status, res.Error)
	assert.Equal(t, stopExited, res.StopReason)
	assert.Equal(t, 1, res.Summary.Samples)
	assert.Nil(t, res.Total)
	names, _ := f.tasks(t)
	assert.Equal(t, []string{taskUp, taskLoad, taskReset, "bench:run:finite"}, names)
}

func TestRunWithoutSamplesHitsMaxDuration(t *testing.T) {
	f := newFakeRun(t)
	res := f.run(t, "silent", runOptions{maxDuration: 300 * time.Millisecond})

	assert.Equal(t, statusNoSamples, res.Status)
	assert.Equal(t, stopMaxDuration, res.StopReason)
	assert.Contains(t, res.Error, "benchmark processor")
	names, _ := f.tasks(t)
	assert.Equal(t, taskDown, names[len(names)-1])
}

func TestRunFailedSetupStillTearsDown(t *testing.T) {
	f := newFakeRun(t)
	t.Setenv("FAKE_TASK_FAIL", taskLoad)
	res := f.run(t, "", runOptions{})

	assert.Equal(t, statusFailed, res.Status)
	assert.Contains(t, res.Error, "task bench:load")
	names, _ := f.tasks(t)
	assert.Equal(t, []string{taskUp, taskLoad, taskDown}, names)
}

func TestRunFailedRunTask(t *testing.T) {
	f := newFakeRun(t)
	t.Setenv("FAKE_TASK_FAIL", "bench:run:finite")
	res := f.run(t, "finite", runOptions{})

	assert.Equal(t, statusFailed, res.Status)
	assert.Equal(t, stopExited, res.StopReason)
	assert.Contains(t, res.Error, "task bench:run:finite")
}

func TestRunMissingPrerequisites(t *testing.T) {
	f := newFakeRun(t)
	t.Setenv("WIDGET_DSN", "")
	f.suite.Requires.Tools = append(f.suite.Requires.Tools, "no-such-tool-for-bench")
	res := f.run(t, "", runOptions{})

	assert.Equal(t, statusFailed, res.Status)
	assert.Contains(t, res.Error, "tool no-such-tool-for-bench is not on PATH")
	assert.Contains(t, res.Error, "environment variable WIDGET_DSN is not set")
	// Nothing was started.
	_, err := os.Stat(f.calls)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRunInterrupted(t *testing.T) {
	f := newFakeRun(t)
	sc, err := f.suite.scenario("silent")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(300*time.Millisecond, cancel)
	res, err := runSuite(ctx, f.root, f.suite, sc, runOptions{outDir: filepath.Join(f.root, "out"), maxDuration: time.Minute}, io.Discard)
	require.NoError(t, err)

	assert.Equal(t, statusFailed, res.Status)
	assert.Equal(t, stopInterrupted, res.StopReason)
	// The suite is torn down all the same.
	names, _ := f.tasks(t)
	assert.Equal(t, taskDown, names[len(names)-1])
}
