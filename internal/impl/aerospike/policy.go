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

package aerospike

import (
	"fmt"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// aoFieldCommitLevel is the configuration field for the write commit policy.
const aoFieldCommitLevel = "commit_level"

// commitLevelField is the write durability choice: wait for replicas, or only
// for the master. Strong-consistency namespaces require `all`.
func commitLevelField() *service.ConfigField {
	return service.NewStringAnnotatedEnumField(aoFieldCommitLevel, map[string]string{
		"all":    "Wait until the master and all replicas have committed. Required for strong-consistency namespaces.",
		"master": "Wait only for the master. Faster in AP namespaces when replica lag is acceptable.",
	}).
		Description("When the client considers a write successful. `all` waits for replica copies; `master` returns after the master copy. Do not use `master` against a strong-consistency namespace.").
		Default("all").
		Advanced()
}

// parseCommitLevel maps the commit_level config value to an Aerospike commit policy.
func parseCommitLevel(s string) (as.CommitLevel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "all", "":
		return as.COMMIT_ALL, nil
	case "master":
		return as.COMMIT_MASTER, nil
	default:
		return 0, fmt.Errorf("unknown commit_level %q: expected all or master", s)
	}
}
