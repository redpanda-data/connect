// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func confluentScenario(t *testing.T) *Scenario {
	t.Helper()
	s, err := LoadScenario("../scenarios/s3/orders-live-confluent.yaml")
	if err != nil {
		t.Fatalf("LoadScenario: %v", err)
	}
	return s
}

func confluentOuts() map[string]string {
	o := s3Outs()
	o["redpanda_schema_registry_url"] = "http://10.42.10.10:8081"
	return o
}

func TestConfluentScenarios_Load(t *testing.T) {
	for _, f := range []string{"orders-live-confluent.yaml", "orders-live-confluent-smoke.yaml"} {
		s, err := LoadScenario("../scenarios/s3/" + f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if kcSpecKey(s) != "s3_confluent" || s.Dataset.Format != "protobuf" {
			t.Errorf("%s: spec %q format %q", f, kcSpecKey(s), s.Dataset.Format)
		}
	}
}

func TestKCSpecKey_DefaultsToConnector(t *testing.T) {
	s, err := LoadScenario("../scenarios/s3/orders-live.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := kcSpecKey(s); got != "s3" {
		t.Errorf("kcSpecKey = %q, want s3", got)
	}
}

func TestS3ConfluentKCConfig_VerbatimWithDocumentedSubstitutions(t *testing.T) {
	s := confluentScenario(t)
	n := newBenchNames("sess", "s3")
	res, err := s3KCConfig(s, confluentOuts(), n)
	if err != nil {
		t.Fatal(err)
	}
	if res.ConnectorName != "bench_s3" {
		t.Errorf("ConnectorName = %q", res.ConnectorName)
	}
	cfg, err := renderKCConfig(s, kcRenderInputs{
		Bucket: "rpcn-bench-results", Region: "us-east-2", Topic: n.SourceTopic(),
		TopicsDir: "raw/x/kafka_connect", SchemaRegistryURL: "http://sr:8081",
	})
	if err != nil {
		t.Fatal(err)
	}
	verbatim := map[string]string{
		"connector.class":                             "io.confluent.connect.s3.S3SinkConnector",
		"consumer.override.auto.offset.reset":         "latest",
		"consumer.override.fetch.max.bytes":           "104857600",
		"consumer.override.fetch.max.wait.ms":         "500",
		"consumer.override.fetch.min.bytes":           "1048576",
		"consumer.override.max.partition.fetch.bytes": "104857600",
		"consumer.override.max.poll.records":          "15000",
		"consumer.override.receive.buffer.bytes":      "104857600",
		"errors.retry.delay.max.ms":                   "60000",
		"errors.retry.timeout":                        "600000",
		"errors.tolerance":                            "all",
		"filename.offset.zero.pad.width":              "20",
		"flush.size":                                  "2000000",
		"format.class":                                "io.confluent.connect.s3.format.parquet.ParquetFormat",
		"key.converter":                               "org.apache.kafka.connect.converters.ByteArrayConverter",
		"locale":                                      "en-US",
		"parquet.codec":                               "zstd",
		"partition.duration.ms":                       "3600000",
		"partitioner.class":                           "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
		"path.format":                                 "'dt'=YYYY'-'MM'-'dd/'hr'=HH'/us-east-1'",
		"rotate.schedule.interval.ms":                 "120000",
		"s3.elastic.buffer.enable":                    "true",
		"s3.elastic.buffer.init.capacity":             "26214400",
		"s3.part.retries":                             "14",
		"s3.part.size":                                "104857600",
		"s3.retry.backoff.ms":                         "1000",
		"schema.compatibility":                        "BACKWARD",
		"storage.class":                               "io.confluent.connect.s3.storage.S3Storage",
		"timestamp.extractor":                         "Record",
		"timezone":                                    "UTC",
		"value.converter":                             "io.confluent.connect.protobuf.ProtobufConverter",
	}
	for k, want := range verbatim {
		if got := cfg[k]; got != want {
			t.Errorf("%s = %v, want %q", k, got, want)
		}
	}
	subst := map[string]string{
		"s3.bucket.name":                      "rpcn-bench-results",
		"s3.region":                           "us-east-2",
		"topics":                              n.SourceTopic(),
		"topics.dir":                          "raw/x/kafka_connect",
		"value.converter.schema.registry.url": "http://sr:8081",
		"tasks.max":                           "__TASKS_MAX__",
	}
	for k, want := range subst {
		if got := cfg[k]; got != want {
			t.Errorf("%s = %v, want %q", k, got, want)
		}
	}
	for _, dropped := range []string{"name", "value.converter.basic.auth.credentials.source", "value.converter.basic.auth.user.info", "offset.flush.interval.ms"} {
		if _, present := cfg[dropped]; present {
			t.Errorf("%s must not be set", dropped)
		}
	}
	// Diagnostic addition from the scenario's kafka_connect.config.
	if cfg["errors.log.enable"] != "true" {
		t.Errorf("errors.log.enable = %v, want true (scenario diagnostics)", cfg["errors.log.enable"])
	}
}

func TestS3ConfluentKCConfig_TopicsDirSitsUnderSidecarPrefix(t *testing.T) {
	s := confluentScenario(t)
	n := newBenchNames("sess", "s3")
	res, err := s3KCConfig(s, confluentOuts(), n)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.ConfigJSON, `"topics.dir":"raw/bench_sess_s3_src/kafka_connect"`) {
		t.Errorf("topics.dir wrong in %s", res.ConfigJSON)
	}
	if !strings.Contains(res.ConfigJSON, `"value.converter.schema.registry.url":"http://10.42.10.10:8081"`) {
		t.Errorf("SR url not substituted in %s", res.ConfigJSON)
	}
	// Connector writes <topics.dir>/<topic>/...; the sidecar lists s3Prefix.
	key := "raw/bench_sess_s3_src/kafka_connect" + "/" + n.SourceTopic() + "/dt=2026-10-01/hr=00/us-east-1/x.parquet"
	if !strings.HasPrefix(key, s3Prefix(n, "kafka_connect")) {
		t.Errorf("object key %q not under sidecar prefix %q", key, s3Prefix(n, "kafka_connect"))
	}
}

func TestMatrixSentinelPatch_NoFlushSentinelIsNoOp(t *testing.T) {
	s := confluentScenario(t)
	res, _ := s3KCConfig(s, confluentOuts(), newBenchNames("sess", "s3"))
	patched := strings.Replace(res.ConfigJSON, `"__TASKS_MAX__"`, `"4"`, 1)
	patched = strings.Replace(patched, `"__FLUSH_INTERVAL_MS__"`, `"2500"`, 1)
	if !strings.Contains(patched, `"tasks.max":"4"`) {
		t.Error("tasks.max sentinel not patched")
	}
	if strings.Contains(patched, "offset.flush.interval.ms") || strings.Contains(patched, "__") {
		t.Errorf("unexpected flush interval or leftover sentinel: %s", patched)
	}
}

func TestSidecar_ConfluentGroupMatchesConnectorName(t *testing.T) {
	// Confluent's framework group is connect-<connector name>; the sidecar
	// greps for the connector name as a substring.
	pattern := s3KafkaConnectConnectorName(newBenchNames("sess", "s3"), 4)
	if !strings.Contains("connect-"+pattern, pattern) || pattern != "bench_s3_v4" {
		t.Errorf("pattern %q", pattern)
	}
}

func TestKafkaConnectBlockValidation(t *testing.T) {
	base := func(kc map[string]any) error {
		s, err := LoadScenario("../scenarios/s3/orders-live.yaml")
		if err != nil {
			t.Fatal(err)
		}
		s.KafkaConnect = kc
		return s.Validate()
	}
	bad := map[string]map[string]any{
		"unknown spec":   {"spec": "nope"},
		"protected prop": {"worker_properties": map[string]any{"bootstrap.servers": "x"}},
		"newline value":  {"worker_properties": map[string]any{"a.b": "x\ny"}},
		"bad key":        {"worker_properties": map[string]any{"a b": "x"}},
		"heap env":       {"env": map[string]any{"KAFKA_HEAP_OPTS": "-Xmx1g"}},
		"bad env name":   {"env": map[string]any{"1BAD": "x"}},
		"jvm not string": {"jvm_performance_opts": 5},
		"props not map":  {"worker_properties": "x"},
	}
	for name, kc := range bad {
		if err := base(kc); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
	if err := base(map[string]any{"spec": "s3_confluent", "env": map[string]any{"A": 1}}); err != nil {
		t.Errorf("valid block rejected: %v", err)
	}
}

func TestRenderKCBenchScript_DefaultScriptByteIdentical(t *testing.T) {
	got := renderKCBenchScript(kcBenchScriptArgs{VCPU: 4, MemLimitGiB: 80, WarmupSec: 600, DurationSec: 900,
		ConnectorName: "bench_s3_v4", ConnectorConfigJSON: `{"connector.class":"x","tasks.max":"4"}`, Bucket: "b", SessionID: "sess",
		ScrapeSetup: "RP=/tmp/x\nRP_SCRAPER=$!", ScrapeUpload: "aws s3 cp x y"})
	want, err := os.ReadFile("testdata/kcscript_default.golden")
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Error("default KC script changed; scenarios without worker overrides must render byte-identically")
	}
}

func confluentScriptArgs(t *testing.T) kcBenchScriptArgs {
	t.Helper()
	s := confluentScenario(t)
	w, err := kcWorkerOverridesFor(s)
	if err != nil {
		t.Fatal(err)
	}
	return kcBenchScriptArgs{VCPU: 2, MemLimitGiB: 40, WarmupSec: 600, DurationSec: 900,
		ConnectorName: "bench_s3_v2", Bucket: "b", SessionID: "sess", Worker: w, ScanWorkerLog: true,
		ConnectorConfigJSON: `{"path.format":"'dt'=YYYY'-'MM'-'dd/'hr'=HH'/us-east-1'"}`}
}

func TestRenderKCBenchScript_WorkerOverrides(t *testing.T) {
	out := renderKCBenchScript(confluentScriptArgs(t))
	for _, want := range []string{
		"cat > /tmp/kc-worker-overrides-2.properties <<'KCWORKER'",
		"offset.flush.interval.ms=15000",
		"connect.protocol=sessioned",
		"consumer.partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor,org.apache.kafka.clients.consumer.StickyAssignor,org.apache.kafka.clients.consumer.RoundRobinAssignor",
		"admin.override.receive.buffer.bytes=16777216",
		"/tmp/kc-worker-2.properties >\"$KC_LOG\"",
		"env KAFKA_HEAP_OPTS=-Xmx20g 'KAFKA_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true' 'AWS_MAX_ATTEMPTS=10' 'AWS_RETRY_MODE=adaptive'",
		"###WARN kc connector/task state",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("script missing %q", want)
		}
	}
	if strings.Contains(out, "schemas.enable") {
		t.Error("worker-level schemas.enable must not be set for this scenario")
	}
	// Single quotes in the connector JSON must reach the file literally.
	if !strings.Contains(out, `"path.format":"'dt'=YYYY'-'MM'-'dd/'hr'=HH'/us-east-1'"`) {
		t.Error("single quotes in connector JSON were altered")
	}
	// The cloud-init worker.properties is only ever read, never rewritten.
	if strings.Contains(out, "> /opt/kafka-connect/worker.properties") || strings.Contains(out, ">> /opt/kafka-connect/worker.properties") {
		t.Error("script must not modify the shared worker.properties")
	}
}

func TestRenderKCBenchScript_ShellSyntaxAndHeredocContent(t *testing.T) {
	out := renderKCBenchScript(confluentScriptArgs(t))
	cmd := exec.Command("sh", "-n")
	cmd.Stdin = strings.NewReader(out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("sh -n: %v\n%s", err, b)
	}
	// Execute just the override-file generation and check the merged result.
	dir := t.TempDir()
	base := dir + "/worker.properties"
	_ = os.WriteFile(base, []byte("bootstrap.servers=a:9092\nfetch.max.bytes=1\nkey.converter.schemas.enable=false\n"), 0o644)
	frag := renderKCWorkerPropsCopy(2, []kcKV{{"fetch.max.bytes", "2097152"}, {"new.key", "v=1"}})
	frag = strings.ReplaceAll(frag, "/opt/kafka-connect/worker.properties", base)
	frag = strings.ReplaceAll(frag, "/tmp/", dir+"/")
	if b, err := exec.Command("sh", "-euc", frag).CombinedOutput(); err != nil {
		t.Fatalf("run fragment: %v\n%s", err, b)
	}
	merged, _ := os.ReadFile(dir + "/kc-worker-2.properties")
	want := "bootstrap.servers=a:9092\nkey.converter.schemas.enable=false\nfetch.max.bytes=2097152\nnew.key=v=1\n"
	if string(merged) != want {
		t.Errorf("merged properties:\n%s\nwant:\n%s", merged, want)
	}
}

func TestRenderKCWorkerLogScan_SurvivesSetEPipefail(t *testing.T) {
	dir := t.TempDir()
	logf := dir + "/kc.log"
	_ = os.WriteFile(logf, []byte("INFO ok\n[x] ERROR Error encountered in task bench-0 Executing stage 'VALUE_CONVERTER'\njava.lang.RuntimeException\n"), 0o644)
	// curl and jq are stubbed through PATH-independent function overrides.
	script := "set -euo pipefail\nKC_LOG=" + logf + "\n" +
		"curl() { echo '{\"connector\":{\"state\":\"RUNNING\"},\"tasks\":[{\"id\":0,\"state\":\"FAILED\"}]}'; }\n" +
		renderKCWorkerLogScan("bench_s3_v1") + "\necho DONE\n"
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not installed")
	}
	b, err := exec.Command("bash", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("scan aborted under set -euo pipefail: %v\n%s", err, b)
	}
	out := string(b)
	if !strings.Contains(out, "###WARN kc connector/task state") || !strings.Contains(out, "###WARN kc worker log has errors") || !strings.Contains(out, "DONE") {
		t.Errorf("unexpected output:\n%s", out)
	}
}

func TestSeedAndWorkloadScripts_ProtobufFlags(t *testing.T) {
	s := confluentScenario(t)
	n := newBenchNames("sess", "s3")
	seed, _ := (sinkTopology{}).SeedScript(s, confluentOuts(), n)
	wl, _ := (sinkTopology{}).WorkloadScript(s, confluentOuts(), n)
	for name, sc := range map[string]string{"seed": seed, "workload": wl} {
		if !strings.Contains(sc, `--format=protobuf --schema-registry-url="http://10.42.10.10:8081"`) {
			t.Errorf("%s script lacks protobuf flags:\n%s", name, sc)
		}
	}
	plain, err := LoadScenario("../scenarios/s3/orders-live.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ps, _ := (sinkTopology{}).SeedScript(plain, s3Outs(), n)
	pw, _ := (sinkTopology{}).WorkloadScript(plain, s3Outs(), n)
	if strings.Contains(ps+pw, "--format") || strings.Contains(ps+pw, "schema-registry") {
		t.Error("JSON scenarios' seed/workload scripts must not gain format flags")
	}
}

func TestConfluentPipelineRendersProcessorsAndParquet(t *testing.T) {
	s := confluentScenario(t)
	outs := confluentOuts()
	outs["redpanda_broker_endpoints"] = "10.0.0.1:9092"
	path, err := renderPipelineConfig(s, outs, sinkTopology{}, newBenchNames("sess", "s3"))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	raw, _ := os.ReadFile(path)
	cfg := string(raw)
	for _, want := range []string{"schema_registry_decode:", "url: http://10.42.10.10:8081", "serialize_to_json: false", "parquet_encode:", "default_compression: zstd", "raw/bench_sess_s3_src/connect/${!uuid_v4()}.parquet"} {
		if !strings.Contains(cfg, want) {
			t.Errorf("rendered config missing %q:\n%s", want, cfg)
		}
	}
	if os.Getenv("BENCH_LINT_CONFIG_OUT") != "" {
		_ = os.WriteFile(os.Getenv("BENCH_LINT_CONFIG_OUT"), raw, 0o644)
	}
}

func TestLowBytesPerRecordWarning(t *testing.T) {
	healthy := []TopicPoint{{MBPerSec: 50, MsgPerSec: 40000, IntervalSec: 10}}
	if w := lowBytesPerRecordWarning("kafka_connect", healthy, 1200); w != "" {
		t.Errorf("healthy point warned: %s", w)
	}
	dropping := []TopicPoint{{MBPerSec: 0.01, MsgPerSec: 40000, IntervalSec: 10}}
	if w := lowBytesPerRecordWarning("kafka_connect", dropping, 1200); !strings.HasPrefix(w, "###WARN") {
		t.Errorf("dropping point not flagged: %q", w)
	}
	if w := lowBytesPerRecordWarning("connect", nil, 1200); w != "" {
		t.Errorf("empty series warned: %q", w)
	}
}
