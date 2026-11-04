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
	"os"
	"os/exec"
	"strings"
)

// dockerCleanup removes all testcontainers containers and their anonymous
// volumes between test packages. Named volumes stay. Skipped in CI
// environments.
func dockerCleanup() {
	if os.Getenv("CI") != "" {
		return
	}

	listCmd := exec.Command("docker", "ps", "-aq", "--filter", "label=org.testcontainers=true")
	out, err := listCmd.Output()
	if err != nil {
		return
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return
	}
	// Images that declare VOLUME, for example MySQL and Postgres, give each
	// container an anonymous volume. Without -v these volumes stay and fill the
	// Docker disk.
	args := append([]string{"rm", "-f", "-v"}, ids...)
	_ = exec.Command("docker", args...).Run()
}
