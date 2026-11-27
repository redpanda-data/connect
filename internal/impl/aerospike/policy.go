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

// Configuration field names for Aerospike read and write policies.
const (
	aoFieldCommitLevel = "commit_level"
	aoFieldReplica     = "replica"
	aoFieldReadModeAP  = "read_mode_ap"
	aoFieldReadModeSC  = "read_mode_sc"
)

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

// readPolicyFields control which copy of a record a lookup may read.
func readPolicyFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringAnnotatedEnumField(aoFieldReplica, map[string]string{
			"sequence":      "Try the master first, then replicas. Client default.",
			"master":        "Read only from the master partition.",
			"master_proles": "Spread reads across master and replica copies. Useful for a hot key when slightly stale data is acceptable.",
			"random":        "Pick a random node. Only when replication factor matches cluster size.",
			"prefer_rack":   "Prefer nodes on the same rack. Requires rack-aware client configuration.",
		}).
			Description("Which partition copy to read from. Match this to the namespace consistency mode; `master_proles` is the usual way to spread a hot read key.").
			Default("sequence").
			Advanced(),

		service.NewStringAnnotatedEnumField(aoFieldReadModeAP, map[string]string{
			"one": "A single replica participates. Default.",
			"all": "Consult duplicate partitions during migration so stale reads are less likely, at extra cost.",
		}).
			Description("Read mode for AP namespaces. `all` is the conservative choice while partitions are migrating.").
			Default("one").
			Advanced(),

		service.NewStringAnnotatedEnumField(aoFieldReadModeSC, map[string]string{
			"session":           "This client sees a monotonic sequence of versions. Default.",
			"linearize":         "All clients see a monotonic sequence. Strongest, and the most expensive.",
			"allow_replica":     "May read from a full replica, not only the master.",
			"allow_unavailable": "May read from a replica even if it is unavailable for writes.",
		}).
			Description("Read mode for strong-consistency namespaces. Leave at `session` unless the workload needs linearizable reads.").
			Default("session").
			Advanced(),
	}
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

// parseReplicaPolicy maps the replica config value to an Aerospike replica policy.
func parseReplicaPolicy(s string) (as.ReplicaPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "sequence", "":
		return as.SEQUENCE, nil
	case "master":
		return as.MASTER, nil
	case "master_proles":
		return as.MASTER_PROLES, nil
	case "random":
		return as.RANDOM, nil
	case "prefer_rack":
		return as.PREFER_RACK, nil
	default:
		return 0, fmt.Errorf("unknown replica %q: expected sequence, master, master_proles, random or prefer_rack", s)
	}
}

// parseReadModeAP maps the read_mode_ap config value.
func parseReadModeAP(s string) (as.ReadModeAP, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "one", "":
		return as.ReadModeAPOne, nil
	case "all":
		return as.ReadModeAPAll, nil
	default:
		return 0, fmt.Errorf("unknown read_mode_ap %q: expected one or all", s)
	}
}

// parseReadModeSC maps the read_mode_sc config value.
func parseReadModeSC(s string) (as.ReadModeSC, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "session", "":
		return as.ReadModeSCSession, nil
	case "linearize":
		return as.ReadModeSCLinearize, nil
	case "allow_replica":
		return as.ReadModeSCAllowReplica, nil
	case "allow_unavailable":
		return as.ReadModeSCAllowUnavailable, nil
	default:
		return 0, fmt.Errorf("unknown read_mode_sc %q: expected session, linearize, allow_replica or allow_unavailable", s)
	}
}

// applyReadPolicy overlays replica and AP/SC read-mode fields onto a batch policy.
func applyReadPolicy(conf *service.ParsedConfig, p *as.BatchPolicy) error {
	replica, err := conf.FieldString(aoFieldReplica)
	if err != nil {
		return err
	}
	if p.ReplicaPolicy, err = parseReplicaPolicy(replica); err != nil {
		return fmt.Errorf("field '%v': %w", aoFieldReplica, err)
	}

	ap, err := conf.FieldString(aoFieldReadModeAP)
	if err != nil {
		return err
	}
	if p.ReadModeAP, err = parseReadModeAP(ap); err != nil {
		return fmt.Errorf("field '%v': %w", aoFieldReadModeAP, err)
	}

	sc, err := conf.FieldString(aoFieldReadModeSC)
	if err != nil {
		return err
	}
	if p.ReadModeSC, err = parseReadModeSC(sc); err != nil {
		return fmt.Errorf("field '%v': %w", aoFieldReadModeSC, err)
	}
	return nil
}
