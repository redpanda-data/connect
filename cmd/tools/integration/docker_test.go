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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDocker puts a docker script on PATH that logs its arguments and lists
// one container and one network. It returns the path of the log file.
func fakeDocker(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	logFile := filepath.Join(dir, "docker.log")
	script := `#!/bin/sh
echo "$@" >> "` + logFile + `"
case "$1 $2" in
  "ps -aq") echo c1 ;;
  "network ls") echo n1 ;;
esac
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logFile
}

func TestDockerCleanupRemovesOnlyTheSession(t *testing.T) {
	logFile := fakeDocker(t)
	t.Setenv("CI", "")

	dockerCleanup("rpcn-it-42-kafka")

	data, err := os.ReadFile(logFile)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ps -aq --filter label=org.testcontainers.sessionId=rpcn-it-42-kafka",
		"rm -f c1",
		"network ls -q --filter label=org.testcontainers.sessionId=rpcn-it-42-kafka",
		"network rm n1",
	}, strings.Split(strings.TrimSpace(string(data)), "\n"))
}

func TestDockerCleanupSkippedInCI(t *testing.T) {
	logFile := fakeDocker(t)
	t.Setenv("CI", "true")

	dockerCleanup("rpcn-it-42-kafka")

	assert.NoFileExists(t, logFile)
}

func TestPackageSessionID(t *testing.T) {
	assert.Equal(t, "rpcn-it-42-aws-s3", packageSessionID(42, "./internal/impl/aws/s3"))
}
