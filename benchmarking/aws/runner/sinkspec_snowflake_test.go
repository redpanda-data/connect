// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"strings"
	"testing"
)

func snowflakeOuts() map[string]string {
	return map[string]string{
		"snowflake_account":         "MYORG-MYACCT",
		"snowflake_user":            "BENCH_USER",
		"snowflake_role":            "BENCH_ROLE",
		"snowflake_database":        "BENCH_DB",
		"snowflake_schema":          "PUBLIC",
		"aws_region":                "us-east-2",
		"redpanda_broker_endpoints": "10.0.0.1:9092",
		"results_bucket":            "rpcn-bench-results",
	}
}

func TestSinkSpecFor_Snowflake(t *testing.T) {
	sp, ok := sinkSpecFor("snowflake")
	if !ok {
		t.Fatal("snowflake sinkSpec must be registered")
	}
	if sp.OutputComponent != "snowflake_streaming" {
		t.Errorf("OutputComponent = %q", sp.OutputComponent)
	}
	if sp.HelperBinary != "snowflake-tablegen" {
		t.Errorf("HelperBinary = %q", sp.HelperBinary)
	}
	if sp.KCConfig != nil {
		t.Error("snowflake is Connect-only; KCConfig must be nil")
	}
}

func TestSinkTopology_Pipeline_Snowflake(t *testing.T) {
	s := &Scenario{Connector: "snowflake", Direction: DirectionSink, Pipeline: map[string]any{
		"output": map[string]any{"snowflake_streaming": map[string]any{
			"max_in_flight": 4,
			"batching":      map[string]any{"count": 50000, "period": "10s"},
		}},
	}}
	in, out, err := (sinkTopology{}).Pipeline(s, newBenchNames("sess", "snowflake"))
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	rp, ok := in["redpanda"].(map[string]any)
	if !ok {
		t.Fatalf("input must be redpanda; got %#v", in)
	}
	topics, _ := rp["topics"].([]any)
	if len(topics) != 1 || topics[0] != "bench_sess_snowflake_src" {
		t.Errorf("input topics = %#v", rp["topics"])
	}
	cfg, ok := out["snowflake_streaming"].(map[string]any)
	if !ok {
		t.Fatalf("output must be snowflake_streaming; got %#v", out)
	}
	// Connection fields are TF-output placeholders resolved at render time.
	for field, want := range map[string]string{
		"account":  "${SNOWFLAKE_ACCOUNT}",
		"user":     "${SNOWFLAKE_USER}",
		"role":     "${SNOWFLAKE_ROLE}",
		"database": "${SNOWFLAKE_DATABASE}",
		"schema":   "${SNOWFLAKE_SCHEMA}",
	} {
		if cfg[field] != want {
			t.Errorf("output %s = %v, want %v", field, cfg[field], want)
		}
	}
	if cfg["table"] != "bench_sess_snowflake_connect" {
		t.Errorf("output table = %v, want bench_sess_snowflake_connect (must match ResetScript/SidecarSetup)", cfg["table"])
	}
	if cfg["private_key_file"] != snowflakeKeyPath {
		t.Errorf("private_key_file = %v, want %v (ResetScript materializes the key there)", cfg["private_key_file"], snowflakeKeyPath)
	}
	// Scenario-owned tuning must survive decoration.
	if cfg["max_in_flight"] != 4 {
		t.Errorf("max_in_flight = %v; DecorateOutput must not clobber scenario tuning", cfg["max_in_flight"])
	}
}

func TestSnowflakeResetScript(t *testing.T) {
	s := &Scenario{Connector: "snowflake", Direction: DirectionSink}
	got, err := (sinkTopology{}).ResetScript(s, snowflakeOuts(), newBenchNames("sess-1", "snowflake"))
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	// The private key materializes before any tablegen call needs it.
	keyFetch := strings.Index(got, "aws ssm get-parameter")
	reset := strings.Index(got, "snowflake-tablegen reset")
	if keyFetch < 0 || reset < 0 || keyFetch > reset {
		t.Errorf("key fetch must precede tablegen reset:\n%s", got)
	}
	if !strings.Contains(got, snowflakeKeySSMParam) {
		t.Errorf("must fetch %s:\n%s", snowflakeKeySSMParam, got)
	}
	if !strings.Contains(got, `chmod 0600 `+snowflakeKeyPath) {
		t.Errorf("key file must be chmod 0600:\n%s", got)
	}
	// Session dashes sanitize to underscores in the table, stay in the group.
	if !strings.Contains(got, `--table="bench_sess_1_snowflake_connect"`) {
		t.Errorf("reset must target the sanitized connect table:\n%s", got)
	}
	if !strings.Contains(got, `--group "bench_sess-1_snowflake_connect"`) {
		t.Errorf("must rewind the connect consumer group:\n%s", got)
	}
	// Connect-only: no kafka_connect tables/groups, no KC connector delete.
	if strings.Contains(got, "kafka_connect") || strings.Contains(got, "connectors/bench_") {
		t.Errorf("Connect-only reset must not touch kafka_connect state:\n%s", got)
	}
	if !strings.Contains(got, `if [ "$attempt" = 3 ]`) {
		t.Errorf("tablegen reset must retry then fail loud:\n%s", got)
	}
}

func TestSnowflakeResetScript_StreamUnion(t *testing.T) {
	s := &Scenario{Connector: "snowflake", Direction: DirectionSink}
	n := newBenchNames("sess", "snowflake").WithStreams(2)
	got, err := (sinkTopology{}).ResetScript(s, snowflakeOuts(), n)
	if err != nil {
		t.Fatalf("ResetScript: %v", err)
	}
	for _, table := range []string{
		"bench_sess_snowflake_connect",
		"bench_sess_snowflake_connect_s0",
		"bench_sess_snowflake_connect_s1",
	} {
		if !strings.Contains(got, `--table="`+table+`"`) {
			t.Errorf("stream-union reset must recreate %s:\n%s", table, got)
		}
	}
}

func TestSnowflakeMetricSidecar(t *testing.T) {
	sc := (sinkTopology{}).MetricSidecar(MetricSidecarArgs{
		Engine:    "connect",
		VCPU:      4,
		Bucket:    "rpcn-bench-results",
		SessionID: "sess",
		Outs:      snowflakeOuts(),
		Names:     newBenchNames("sess", "snowflake"),
	})
	if !strings.Contains(sc.Setup, "RP=/tmp/snowflake-4-connect.txt") {
		t.Errorf("sidecar must write the snowflake-prefixed artifact:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "snowflake-tablegen poll") {
		t.Errorf("sidecar must poll via snowflake-tablegen:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "--tables=bench_sess_snowflake_connect") {
		t.Errorf("sidecar must poll the connect table:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Setup, "RP_SCRAPER=$!") {
		t.Errorf("sidecar setup must end by exporting RP_SCRAPER:\n%s", sc.Setup)
	}
	if !strings.Contains(sc.Upload, "s3://rpcn-bench-results/runs/sess/snowflake-4-connect.txt") {
		t.Errorf("upload key mismatch:\n%s", sc.Upload)
	}
}

func TestSinkTopology_KCConfig_SnowflakeHasNoCounterpart(t *testing.T) {
	s := &Scenario{Connector: "snowflake", Direction: DirectionSink}
	_, ok, err := (sinkTopology{}).KCConfig(s, snowflakeOuts(), newBenchNames("sess", "snowflake"))
	if err != nil {
		t.Fatalf("KCConfig: %v", err)
	}
	if ok {
		t.Error("snowflake is Connect-only; KCConfig must report ok=false so --engines=kafka_connect fails up front")
	}
}

func TestSinkTopology_MetricArtifact_Snowflake(t *testing.T) {
	if got := (sinkTopology{}).MetricArtifact("snowflake", "connect", "2"); got != "snowflake-2-connect.txt" {
		t.Errorf("MetricArtifact = %q, want snowflake-2-connect.txt", got)
	}
}

// End-to-end render over the real scenario files: LoadScenario validates
// them, renderPointConfigs proves every ${SNOWFLAKE_*} placeholder resolves
// against the snowflake stack's outputs and the key file path survives to
// the final YAML.
func TestRenderPointConfigs_SnowflakeScenarios(t *testing.T) {
	for _, path := range []string{
		"../scenarios/snowflake/orders-sink-smoke.yaml",
		"../scenarios/snowflake/orders-sink.yaml",
	} {
		s, err := LoadScenario(path)
		if err != nil {
			t.Fatalf("LoadScenario(%s): %v", path, err)
		}
		topo, err := topologyFor(s.Direction)
		if err != nil {
			t.Fatalf("topologyFor: %v", err)
		}
		names := newBenchNames("sess-x", s.Connector)
		got, err := renderPointConfigs(s, snowflakeOuts(), topo, names, buildSweepPlan(s)[0])
		if err != nil {
			t.Fatalf("renderPointConfigs(%s): %v", path, err)
		}
		cfg := readYAML(t, got.Single)
		sfCfg := cfg["output"].(map[string]any)["snowflake_streaming"].(map[string]any)
		if sfCfg["account"] != "MYORG-MYACCT" {
			t.Errorf("%s: account = %v; ${SNOWFLAKE_ACCOUNT} must resolve from TF outputs", path, sfCfg["account"])
		}
		if sfCfg["table"] != "bench_sess_x_snowflake_connect" {
			t.Errorf("%s: table = %v", path, sfCfg["table"])
		}
		if sfCfg["private_key_file"] != snowflakeKeyPath {
			t.Errorf("%s: private_key_file = %v", path, sfCfg["private_key_file"])
		}
		if se, ok := sfCfg["schema_evolution"].(map[string]any); !ok || se["enabled"] != false {
			t.Errorf("%s: schema_evolution must stay disabled as the scenario declares; got %v", path, sfCfg["schema_evolution"])
		}
	}
}

func TestValidateEngines_ConnectOnlySink(t *testing.T) {
	s := &Scenario{Connector: "snowflake", Direction: DirectionSink}
	if err := validateEngines(s, []string{"connect", "kafka_connect"}); err == nil {
		t.Error("default engine list must fail fast for a Connect-only sink")
	}
	if err := validateEngines(s, []string{"connect"}); err != nil {
		t.Errorf("engines=connect must pass: %v", err)
	}
	// iceberg has a KC counterpart; both engines stay valid.
	if err := validateEngines(&Scenario{Connector: "iceberg", Direction: DirectionSink}, []string{"connect", "kafka_connect"}); err != nil {
		t.Errorf("iceberg dual-engine must pass: %v", err)
	}
}
