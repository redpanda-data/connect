// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"strings"
)

// Trust boundary: scenario YAML and terraform outputs are operator-controlled.
// Renderers below quote values with %q (Go double-quoted string), which is NOT
// shell-safe — bash still expands $, ` and \ inside "...". This is intentional
// because (a) the RDS modules generate passwords with special=false (alphanumeric
// only), and (b) reset SQL is hand-authored. If you ever feed user input into
// these renderers, switch to a real shell-quoter.

// renderSeedScript renders the shell script that runs on the load-gen host to
// pre-seed the source database. The seeder is expected to be staged at
// /opt/bench/<seeder> by the time this runs.
func renderSeedScript(s *Scenario, outs map[string]string, s3Key string) (string, error) {
	es, ok := engineSpecFor(s.Connector)
	if !ok {
		return "", fmt.Errorf("renderSeedScript: connector %q has no engineSpec", s.Connector)
	}
	return fmt.Sprintf(`
set -euo pipefail
aws s3 cp s3://%s/%s /opt/bench/%s
chmod +x /opt/bench/%s
%s=%q /opt/bench/%s seed \
  --tables=%s --rows=%d --row-size=%d
`,
		outs["results_bucket"], s3Key, s.Dataset.Seeder, s.Dataset.Seeder,
		es.DSNEnvVar, outs[es.DSNOutputKey], s.Dataset.Seeder,
		strings.Join(s.Dataset.Tables, ","), s.Dataset.InitialRows, s.Dataset.RowSizeBytes,
	), nil
}

// combineReset builds the shell script that runs between every sweep point to
// restore a known state (drop a slot, truncate a table, etc.).
func combineReset(connector string, steps []ResetStep, outs map[string]string) (string, error) {
	if len(steps) == 0 {
		return "", nil
	}
	es, ok := engineSpecFor(connector)
	if !ok {
		return "", fmt.Errorf("combineReset: connector %q has no engineSpec", connector)
	}
	var sb strings.Builder
	sb.WriteString("set -euo pipefail\n")
	for _, st := range steps {
		if st.SQL != "" {
			if es.ResetHostOutputKey != "" {
				// Discrete-flags form (mysql).
				sb.WriteString(fmt.Sprintf(
					`mysql -h %q -P %q -u %q -p%q %q -e %q`+"\n",
					outs[es.ResetHostOutputKey],
					outs[es.ResetPortOutputKey],
					outs[es.ResetUserOutputKey],
					outs[es.ResetPassOutputKey],
					outs[es.ResetDBOutputKey],
					st.SQL,
				))
			} else {
				// DSN form (postgres).
				sb.WriteString(fmt.Sprintf(
					`psql %q -v ON_ERROR_STOP=1 -c %q`+"\n",
					outs[es.DSNOutputKey], st.SQL,
				))
			}
		}
		if st.Bash != "" {
			sb.WriteString(substitutePlaceholders(st.Bash, outs) + "\n")
		}
	}
	return sb.String(), nil
}

// renderWorkloadScript renders the shell script that runs on the load-gen host
// to drive sustained writes while Connect is reading on the runner host.
// Returns "" (no error) when no workload is configured.
func renderWorkloadScript(s *Scenario, outs map[string]string) (string, error) {
	if s.Workload == nil {
		return "", nil
	}
	es, ok := engineSpecFor(s.Connector)
	if !ok {
		return "", fmt.Errorf("renderWorkloadScript: connector %q has no engineSpec", s.Connector)
	}
	totalSec := int((s.Workload.Warmup + s.Workload.Duration).Seconds())
	return fmt.Sprintf(`
set -euo pipefail
%s=%q /opt/bench/%s workload \
  --tables=%s --row-size=%d \
  --rate=%d --duration=%ds
`,
		es.DSNEnvVar, outs[es.DSNOutputKey], s.Dataset.Seeder,
		strings.Join(s.Dataset.Tables, ","),
		s.Dataset.RowSizeBytes,
		s.Workload.WriteRatePerSec,
		totalSec,
	), nil
}
