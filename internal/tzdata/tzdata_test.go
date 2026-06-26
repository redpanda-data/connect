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

package tzdata_test

import (
	"os/exec"
	"strings"
	"testing"
)

// shippingEntrypoints are the main packages whose binaries are distributed to
// users. Each must embed the tz database so time.LoadLocation works in the
// stripped release container images, which carry no system zoneinfo.
var shippingEntrypoints = []string{
	"github.com/redpanda-data/connect/v4/cmd/redpanda-connect",
	"github.com/redpanda-data/connect/v4/cmd/redpanda-connect-ai",
	"github.com/redpanda-data/connect/v4/cmd/redpanda-connect-cloud",
	"github.com/redpanda-data/connect/v4/cmd/redpanda-connect-community",
	"github.com/redpanda-data/connect/v4/cmd/serverless/connect-lambda",
}

// TestEntrypointsEmbedTzdata asserts every shipping binary pulls in time/tzdata
// (transitively, via internal/tzdata). This is the host-independent guard
// against a stripped image silently regressing LoadLocation to UTC: it fails if
// a future change drops the embed, rather than relying on the host's tz
// database (which would mask the regression on developer machines).
func TestEntrypointsEmbedTzdata(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go list dependency scan in -short mode")
	}
	for _, pkg := range shippingEntrypoints {
		t.Run(pkg, func(t *testing.T) {
			out, err := exec.Command("go", "list", "-deps", pkg).CombinedOutput()
			if err != nil {
				t.Fatalf("go list -deps %s: %v\n%s", pkg, err, out)
			}
			if !containsLine(string(out), "time/tzdata") {
				t.Errorf("%s does not embed time/tzdata; blank-import internal/tzdata in its main package", pkg)
			}
		})
	}
}

func containsLine(out, want string) bool {
	for line := range strings.SplitSeq(out, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}
