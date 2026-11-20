// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"gopkg.in/yaml.v3"
)

// A stack-none e2e scenario has no source system. Its input reads
// bench_<session>_src on the bench Redpanda cluster, which a sustained
// producer fills for the whole of every sweep point. The topic is sized to
// fit the brokers' disks (3 x 100 GB) rather than preloaded: retention caps it
// at 4 GiB per partition, so 12 partitions hold about 48 GiB per replica.
const (
	sourcePartitions     = 12
	sourceReplicas       = 3
	sourceRetentionBytes = int64(4) << 30
	sourceRetentionMs    = 1_800_000

	// The producer runs on the runner host, pinned clear of the Connect
	// under test, which takes cores 2..(1+VCPU) and so at most 2-9 on the
	// 16-core c8g.4xlarge. The load-gen host is a c8g.large (2 vCPU, under
	// 1 Gbps baseline network) and could not sustain the producer rate.
	producerCores = "10-15"
	// maxStackNoneVCPU is the largest sweep point that stays clear of
	// producerCores: Connect takes cores 2..(1+VCPU).
	maxStackNoneVCPU   = 8
	producerGOMAXPROCS = 6
	producerBinary     = "/opt/bench/redpanda-connect"
	producerHeredoc    = "BENCH_PRODUCER_EOF"
	producerRateLabel  = "bench_producer_rate"

	// producerFrameBytes is the fixed part of each generated message,
	// {"id":<int space-padded to 12>,"pad":"<pad>"}, so the pad is
	// row_size_bytes minus it. Spaces, not zeros: a leading zero is not
	// valid JSON.
	producerFrameBytes = len(`{"id":`) + 12 + len(`,"pad":"`) + len(`"}`)
)

// ProducerArgs is the render context for renderProducerScript.
type ProducerArgs struct {
	Brokers      string // comma-separated host:port list from redpanda_broker_endpoints
	Topic        string
	RatePerSec   int
	RowSizeBytes int
	TotalSec     int // warmup + duration, the same total the CDC workloads use
}

// renderProducerScript renders the workload script of a stack-none scenario.
// It runs a Connect process for TotalSec that generates JSON objects of
// RowSizeBytes, throttled to RatePerSec by a local rate limit counted per
// 100ms, into a redpanda output on Topic.
func renderProducerScript(a ProducerArgs) string {
	pad := strings.Repeat("x", max(0, a.RowSizeBytes-producerFrameBytes))
	mapping := fmt.Sprintf(`root = "{\"id\":" + "%%12d".format(counter()) + ",\"pad\":\"%s\"}"`, pad)
	cfg := map[string]any{
		// Port 4195 belongs to the Connect under test on the same host.
		"http":   map[string]any{"enabled": false},
		"logger": map[string]any{"level": "WARN"},
		"input": map[string]any{"generate": map[string]any{
			"mapping":    mapping,
			"interval":   "",
			"count":      0,
			"batch_size": 100,
		}},
		"pipeline": map[string]any{"processors": []any{
			map[string]any{"rate_limit": map[string]any{"resource": producerRateLabel}},
		}},
		"output": map[string]any{"redpanda": map[string]any{
			"seed_brokers":  []any{a.Brokers},
			"topic":         a.Topic,
			"max_in_flight": 64,
			// The pad is a run of one byte: compression would shrink what
			// the brokers store and serve far below what a customer's data
			// would.
			"compression": "none",
			"batching":    map[string]any{"count": 1000, "period": "10ms"},
		}},
		"rate_limit_resources": []any{map[string]any{
			"label": producerRateLabel,
			"local": map[string]any{
				"count":    (a.RatePerSec + 9) / 10,
				"interval": "100ms",
			},
		}},
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		// Only plain maps, strings and ints: Marshal cannot fail.
		panic(err)
	}
	// timeout exits 124 when it stops the producer on schedule; that is the
	// expected end, any other non-zero exit is a producer failure.
	return fmt.Sprintf(`
set -euo pipefail
CFG=/tmp/bench-producer.yaml
PLOG=/tmp/bench-producer.log
cat > "$CFG" <<'%[1]s'
%[2]s%[1]s
echo "producer: %[3]d msg/s of %[4]d B to %[5]s for %[6]ds on cores %[7]s"
rc=0
timeout --signal=TERM %[6]ds taskset -c %[7]s env GOMAXPROCS=%[8]d REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %[9]s run "$CFG" >"$PLOG" 2>&1 || rc=$?
if [ "$rc" -ne 0 ] && [ "$rc" -ne 124 ]; then
  echo "producer exited $rc"
  tail -n 50 "$PLOG"
  exit "$rc"
fi
echo "producer finished"
`, producerHeredoc, raw, a.RatePerSec, a.RowSizeBytes, a.Topic, a.TotalSec,
		producerCores, producerGOMAXPROCS, producerBinary)
}

// renderSourceTopicScript renders the seed step of a stack-none scenario:
// create the source topic. Retention bounds what the producer leaves on the
// brokers across every point of the sweep.
func renderSourceTopicScript(brokers, topic string) string {
	return fmt.Sprintf(`
set -euo pipefail
/usr/local/bin/rpk topic create %q -p %d -r %d -c retention.bytes=%d -c retention.ms=%d -X brokers=%q
`, topic, sourcePartitions, sourceReplicas, sourceRetentionBytes, sourceRetentionMs, brokers)
}

// sourceBrokers returns the broker endpoints a stack-none scenario's producer
// and topic setup talk to. They come from the terraform output, never
// localhost: the brokers are a separate cluster.
func sourceBrokers(outs map[string]string) (string, error) {
	b := outs["redpanda_broker_endpoints"]
	if b == "" {
		return "", errors.New("stack none: terraform output redpanda_broker_endpoints is missing or empty")
	}
	return b, nil
}

// producerArgsFor builds the producer's render context from a stack-none
// scenario.
func producerArgsFor(s *Scenario, outs map[string]string, n BenchNames) (ProducerArgs, error) {
	brokers, err := sourceBrokers(outs)
	if err != nil {
		return ProducerArgs{}, err
	}
	if s.Workload == nil {
		return ProducerArgs{}, errors.New("stack none: workload is required")
	}
	return ProducerArgs{
		Brokers:      brokers,
		Topic:        n.SourceTopic(),
		RatePerSec:   s.Workload.WriteRatePerSec,
		RowSizeBytes: s.Dataset.RowSizeBytes,
		TotalSec:     int((s.Workload.Warmup + s.Workload.Duration).Seconds()),
	}, nil
}

// pointOuts returns a copy of outs carrying the per-point values of the
// source placeholders a plan's input uses: ${BENCH_REDPANDA_BROKERS},
// ${BENCH_TOPIC} and ${BENCH_CONSUMER_GROUP}. The consumer group is unique
// per point, so every point reads the topic from its oldest retained offset.
func pointOuts(outs map[string]string, n BenchNames, pointKey string) map[string]string {
	out := maps.Clone(outs)
	if out == nil {
		out = map[string]string{}
	}
	out["bench_redpanda_brokers"] = outs["redpanda_broker_endpoints"]
	out["bench_topic"] = n.SourceTopic()
	out["bench_consumer_group"] = n.ConsumerGroup(pointKey)
	return out
}

// perPointConfigs reports whether a scenario renders one config per sweep
// point rather than one shared by all of them: arms differ in pipeline, and
// stack none differs in consumer group.
func perPointConfigs(s *Scenario) bool {
	return len(s.Matrix.Arms) > 0 || s.Stack == stackNone
}

// workloadInstance is the host a scenario's seed and workload scripts run
// on: the load-gen host, except for stack none, whose producer runs pinned
// on the runner host (see producerCores).
func workloadInstance(s *Scenario, outs map[string]string) string {
	if s.Stack == stackNone {
		return outs["runner_instance_id"]
	}
	return outs["load_gen_instance_id"]
}

// stackTF is the part of Terraform runBench and downCmd use on the
// per-scenario stack.
type stackTF interface {
	Init() error
	Apply(vars map[string]string) error
	Destroy(vars map[string]string) error
	Outputs() (map[string]string, error)
}

// noStackTerraform stands in for the stack terraform of a stack-none
// scenario, which has no source system to provision.
type noStackTerraform struct{}

func (noStackTerraform) Init() error                         { return nil }
func (noStackTerraform) Apply(map[string]string) error       { return nil }
func (noStackTerraform) Destroy(map[string]string) error     { return nil }
func (noStackTerraform) Outputs() (map[string]string, error) { return map[string]string{}, nil }

// stackTerraform returns the per-scenario stack's terraform, or a no-op for
// stack none.
func stackTerraform(s *Scenario, repoRoot, backendFile string) stackTF {
	if s.Stack == stackNone {
		return noStackTerraform{}
	}
	return &Terraform{
		Dir:         StackDir(repoRoot, s.Stack),
		BackendFile: backendFile,
		StateKey:    s.Stack,
	}
}
