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
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// sessionLabel is the label that testcontainers-go puts on each container and
// network it starts. Its value is TESTCONTAINERS_SESSION_ID when that is set.
const sessionLabel = "org.testcontainers.sessionId"

// packageSessionID returns the testcontainers session ID for one package of
// this runner process. The runner sets it as TESTCONTAINERS_SESSION_ID, so that
// dockerCleanup can find the containers of that package and no others.
//
// testcontainers-go already gives each go test process its own session ID, but
// it derives that ID from the parent PID, so the runner cannot know it.
func packageSessionID(runnerPID int, pkgPath string) string {
	return fmt.Sprintf("rpcn-it-%d-%s", runnerPID, strings.TrimSuffix(pkgFilename(pkgPath), ".txt"))
}

// dockerCleanup removes the containers, their anonymous volumes, and the
// networks of one testcontainers session after its package is done. Containers
// of other sessions, for example other packages that run at the same time,
// stay. Named volumes stay too. Skipped in CI environments.
func dockerCleanup(sessionID string) {
	if os.Getenv("CI") != "" {
		return
	}

	filter := "label=" + sessionLabel + "=" + sessionID
	// Remove the containers first, because Docker does not remove a network
	// that a container still uses. Images that declare VOLUME, for example
	// MySQL and Postgres, give each container an anonymous volume, and without
	// -v these volumes stay and fill the Docker disk.
	dockerRemove([]string{"ps", "-aq", "--filter", filter}, "rm", "-f", "-v")
	dockerRemove([]string{"network", "ls", "-q", "--filter", filter}, "network", "rm")
}

// dockerRemove runs the list command and then the remove command on the IDs
// that it prints. It does nothing when the list is empty or fails.
func dockerRemove(list []string, remove ...string) {
	out, err := exec.Command("docker", list...).Output()
	if err != nil {
		return
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return
	}
	_ = exec.Command("docker", append(remove, ids...)...).Run()
}
