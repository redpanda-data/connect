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

//go:build unix

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeGo puts a go script on PATH that acts like go test on SIGINT: it ignores
// the signal and waits for a fake test binary. The fake test binary writes
// "started" to the log file, and "interrupted" when it gets SIGINT. It stops
// by itself after about 10 seconds, so that a failed test leaves no process.
// It returns the path of the log file.
func fakeGo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	logFile := filepath.Join(dir, "go.log")
	// Keep "trap : INT". With "trap '' INT", the fake test binary starts with
	// SIGINT ignored, and a shell cannot trap a signal that was ignored on entry.
	// Keep the short sleep loop too. With one "sleep 10", the trap did not run
	// on every try.
	const script = `#!/bin/sh
trap : INT
sh -c 'trap "echo interrupted >> \"$FAKE_GO_LOG\"; exit 2" INT
echo started >> "$FAKE_GO_LOG"
i=0; while [ $i -lt 200 ]; do sleep 0.05; i=$((i+1)); done'
exit 1
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go"), []byte(script), 0o755))
	t.Setenv("FAKE_GO_LOG", logFile)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logFile
}

func TestRunPackageTestInterruptStopsTestBinary(t *testing.T) {
	logFile := fakeGo(t)
	outDir := t.TempDir()
	out := NewOutput(io.Discard, filepath.Join(outDir, "index.md"))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan packageResult)
	go func() {
		done <- runPackageTest(ctx, TestPackage{Path: "./internal/impl/kafka"}, "kafka",
			filepath.Join(outDir, "kafka.txt"), PackageCache{}, out)
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(logFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, "fake test binary did not start")
	cancel()

	select {
	case res := <-done:
		assert.Equal(t, ResultFail, res.status)
	case <-time.After(5 * time.Second):
		t.Fatal("runPackageTest did not return after the context was canceled")
	}
	data, err := os.ReadFile(logFile)
	require.NoError(t, err)
	assert.Equal(t, "started\ninterrupted\n", string(data))
}
