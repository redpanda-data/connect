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

// Package cdctest deterministically checks that every registered CDC input
// (any input whose name ends in _cdc or _changefeed) exposes the settled
// canonical config field names. The CDC fleet is discovered at runtime from
// the registry, so adding or renaming a connector automatically includes or
// excludes it — there is no hand-maintained connector list.
//
// The check is non-breaking: connectors that have not yet converged are listed
// in knownNonConformant with a per-field reason. New connectors are strict by
// default. Remove entries from knownNonConformant as connectors migrate
// (tracked on the CON CDC-convergence epic).
package cdctest

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import the full bundle so every CDC input registers itself.
	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// cdcNameRE matches the registered names of CDC inputs. This is the
// self-maintaining fleet detector — no hardcoded connector list.
var cdcNameRE = regexp.MustCompile(`_(cdc|changefeed)$`)

// Canonical CDC config field names (the settled subset). Open-decision names
// are intentionally NOT asserted yet:
//   - TODO(cdc-standard): metadata key table vs table_name (OD-1)
//   - TODO(cdc-standard): snapshot enable shape bool stream_snapshot vs enum
//     snapshot_mode (OD-2) — connectors using the enum should waive
//     stream_snapshot until a single canonical name is chosen.
var canonicalFields = []string{
	"checkpoint_cache",
	"checkpoint_limit",
	"snapshot_max_batch_size",
	"max_parallel_snapshot_tables",
	"stream_snapshot",
}

// knownNonConformant waives specific (connector → field → reason) checks that
// have not yet been migrated. New connectors default to strict. Populated from
// the actual registry state; shrink it as connectors converge.
//
// Fully conformant today (intentionally absent): microsoft_sql_server_cdc,
// mysql_cdc, oracledb_cdc.
var knownNonConformant = map[string]map[string]string{
	"aws_dynamodb_cdc": {
		"checkpoint_cache":             "uses checkpoint_table; migrate to checkpoint_cache",
		"snapshot_max_batch_size":      "uses snapshot_batch_size; migrate",
		"max_parallel_snapshot_tables": "uses snapshot segments; migrate",
		"stream_snapshot":              "uses enum snapshot_mode; pending OD-2 (bool vs enum)",
	},
	"cockroachdb_changefeed": {
		"checkpoint_cache":             "uses cursor_cache; migrate to checkpoint_cache",
		"checkpoint_limit":             "not yet exposed under the canonical name",
		"snapshot_max_batch_size":      "no distinct snapshot phase (changefeed initial scan)",
		"max_parallel_snapshot_tables": "no distinct snapshot phase (changefeed initial scan)",
		"stream_snapshot":              "no distinct snapshot phase (changefeed initial scan)",
	},
	"gcp_spanner_cdc": {
		"checkpoint_cache":             "not yet exposed under the canonical name",
		"checkpoint_limit":             "not yet exposed under the canonical name",
		"snapshot_max_batch_size":      "not yet exposed under the canonical name",
		"max_parallel_snapshot_tables": "not yet exposed under the canonical name",
		"stream_snapshot":              "not yet exposed under the canonical name",
	},
	"mongodb_cdc": {
		"snapshot_max_batch_size":      "snapshot batch size not exposed under the canonical name",
		"max_parallel_snapshot_tables": "uses snapshot_parallelism; migrate",
	},
	"postgres_cdc": {
		"checkpoint_cache":        "uses native replication-slot checkpointing by design",
		"snapshot_max_batch_size": "uses snapshot_batch_size; migrate",
	},
	"salesforce_cdc": {
		"max_parallel_snapshot_tables": "uses max_parallel_snapshot_objects; migrate",
	},
}

// componentSchema is the subset of the docs.ComponentSpec JSON emitted by
// service.ConfigView.FormatJSON that we need: the top-level config field names
// live at config.children[].name.
type componentSchema struct {
	Config struct {
		Children []struct {
			Name string `json:"name"`
		} `json:"children"`
	} `json:"config"`
}

func TestCDCConformance(t *testing.T) {
	env := service.GlobalEnvironment()

	type cdcInput struct {
		name   string
		fields map[string]struct{}
	}
	var inputs []cdcInput

	var walkErr error
	env.WalkInputs(func(name string, view *service.ConfigView) {
		if walkErr != nil || !cdcNameRE.MatchString(name) {
			return
		}
		raw, err := view.FormatJSON()
		if err != nil {
			walkErr = err
			return
		}
		var cs componentSchema
		if err := json.Unmarshal(raw, &cs); err != nil {
			walkErr = err
			return
		}
		fields := make(map[string]struct{}, len(cs.Config.Children))
		for _, c := range cs.Config.Children {
			fields[c.Name] = struct{}{}
		}
		inputs = append(inputs, cdcInput{name: name, fields: fields})
	})
	if walkErr != nil {
		t.Fatalf("introspecting CDC input schema: %v", walkErr)
	}
	if len(inputs) == 0 {
		t.Fatal("no CDC inputs (_cdc/_changefeed) found; is the components/all bundle imported?")
	}

	for _, in := range inputs {
		t.Run(in.name, func(t *testing.T) {
			if !strings.HasSuffix(in.name, "_cdc") && !strings.HasSuffix(in.name, "_changefeed") {
				t.Errorf("CDC input name %q must end with _cdc or _changefeed", in.name)
			}
			waived := knownNonConformant[in.name]
			for _, f := range canonicalFields {
				if reason, ok := waived[f]; ok {
					t.Logf("WAIVED %s.%s: %s", in.name, f, reason)
					continue
				}
				if _, ok := in.fields[f]; !ok {
					t.Errorf("missing canonical field %q (if migration is pending, add to knownNonConformant with a reason)", f)
				}
			}
			for f, reason := range waived {
				if _, ok := in.fields[f]; ok {
					t.Logf("READY: %s.%s now present (%q) — remove from knownNonConformant", in.name, f, reason)
				}
			}
		})
	}

	// Stale-entry guard: every knownNonConformant key must be a current CDC input.
	registered := make(map[string]struct{}, len(inputs))
	for _, in := range inputs {
		registered[in.name] = struct{}{}
	}
	for name := range knownNonConformant {
		if _, ok := registered[name]; !ok {
			t.Errorf("stale knownNonConformant entry %q is not a registered CDC input; remove it", name)
		}
	}
}
