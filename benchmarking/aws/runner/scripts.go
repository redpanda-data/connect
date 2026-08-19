// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"fmt"
	"sort"
	"strings"
)

// envVarPrefix builds the `KEY="value" ...` prefix used in front of the seeder
// and workload commands. ExtraEnvVars keys are emitted in sorted order, then
// the DSN env var (unless es.NoDSN is set). Returns empty string when both
// sources are empty.
func envVarPrefix(es engineSpec, outs map[string]string) string {
	keys := make([]string, 0, len(es.ExtraEnvVars))
	for k := range es.ExtraEnvVars {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s=%q ", k, outs[es.ExtraEnvVars[k]])
	}
	if !es.NoDSN && es.DSNEnvVar != "" {
		fmt.Fprintf(&sb, "%s=%q ", es.DSNEnvVar, outs[es.DSNOutputKey])
	}
	return sb.String()
}

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
%s/opt/bench/%s seed \
  --tables=%s --rows=%d --row-size=%d
`,
		outs["results_bucket"], s3Key, s.Dataset.Seeder, s.Dataset.Seeder,
		envVarPrefix(es, outs), s.Dataset.Seeder,
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
	if es.NoDSN {
		for _, st := range steps {
			if st.SQL != "" {
				return "", fmt.Errorf("combineReset: connector %q is NoDSN; sql: reset steps not supported (use bash: steps)", connector)
			}
		}
	}
	var sb strings.Builder
	sb.WriteString("set -euo pipefail\n")
	for _, st := range steps {
		if st.SQL != "" {
			// DSN form (postgres). The discrete host/port/user/pass/db-flags
			// form (mysql's `mysql -h ... -P ... -e ...`) was dead once the
			// registry was trimmed to postgres_cdc only — no remaining
			// engineSpec entry sets ResetHostOutputKey — and returns with
			// mysql_cdc's own stack PR.
			sb.WriteString(fmt.Sprintf(
				`psql %q -v ON_ERROR_STOP=1 -c %q`+"\n",
				outs[es.DSNOutputKey], st.SQL,
			))
		}
		if st.Bash != "" {
			sb.WriteString(substitutePlaceholders(st.Bash, outs) + "\n")
		}
	}
	// Between sweep points, Connect's per-session output topic must be torn
	// down so the next point starts from a clean baseline. Gated on
	// session+brokers because pre-Plan-2 callers (and unit tests) may not
	// populate them.
	sessionID := outs["bench_session_id"]
	brokers := outs["redpanda_broker_endpoints"]
	if sessionID != "" && brokers != "" {
		connectTopic := fmt.Sprintf("bench_%s_%s_connect", sessionID, connector)
		// rpk is installed on the runner host by runner-user-data.tftpl
		// (Kafka's CLI is only present when install_kc is set). Regex mode
		// (-r) makes the first point — before Connect's first write
		// auto-creates the topic — a clean no-op. A broker-connectivity
		// failure exits non-zero and aborts the reset under `set -euo
		// pipefail`; a per-topic delete error exits 0 but rpk prints it in
		// the status table, which lands in the streamed reset log rather
		// than being discarded. Session IDs and connector names are
		// [A-Za-z0-9_-], so the anchored literal needs no regex escaping.
		sb.WriteString(fmt.Sprintf(
			`/usr/local/bin/rpk topic delete -r %q -X brokers=%q`+"\n",
			"^"+connectTopic+"$", brokers,
		))
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
%s/opt/bench/%s workload \
  --tables=%s --row-size=%d \
  --rate=%d --duration=%ds
`,
		envVarPrefix(es, outs), s.Dataset.Seeder,
		strings.Join(s.Dataset.Tables, ","),
		s.Dataset.RowSizeBytes,
		s.Workload.WriteRatePerSec,
		totalSec,
	), nil
}
