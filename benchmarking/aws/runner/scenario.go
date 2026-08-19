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
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	minWarmup           = 2 * time.Minute
	minDuration         = 15 * time.Minute
	reservedCores       = 2
	defaultGoMemPerVCPU = 2 // GiB
	// maxArmGOMAXPROCS bounds Arm.GOMAXPROCS. The taskset core pin always
	// follows vCPU, not GOMAXPROCS (see renderBenchScript), so oversubscribing
	// past the largest vCPU count any supported instance type offers
	// (instanceTypeVCPU's max, the 16xlarge row) buys nothing but scheduler
	// contention on cores the arm was never pinned to. A typo like
	// `gomaxprocs: 1000` would otherwise validate fine and silently
	// oversubscribe by 15x with no error until the AWS run is already paying
	// for it.
	maxArmGOMAXPROCS = 64
	// maxArmSweepPoints bounds len(matrix.cpu_points) * len(matrix.arms). Arms
	// used to require exactly one cpu_points entry; lifting that restriction
	// (topology × core count is the whole point of the 7-table consolidation
	// test) still needs a ceiling so a careless scenario can't silently
	// commit to a day-long run.
	maxArmSweepPoints = 8
)

// armIDRe constrains arm ids to what is safe in a filename and an S3 key.
var armIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// instanceTypeVCPU maps known EC2 instance types to their vCPU counts.
// Extend this table when new types are referenced by scenarios.
var instanceTypeVCPU = map[string]int{
	// c7i (Intel x86_64) — kept for backward compatibility with existing fixtures
	"c7i.large":    2,
	"c7i.xlarge":   4,
	"c7i.2xlarge":  8,
	"c7i.4xlarge":  16,
	"c7i.8xlarge":  32,
	"c7i.12xlarge": 48,
	"c7i.16xlarge": 64,
	// c8g (Graviton arm64) — matches arm64 AMI and arm64 Go build target
	"c8g.large":    2,
	"c8g.xlarge":   4,
	"c8g.2xlarge":  8,
	"c8g.4xlarge":  16,
	"c8g.8xlarge":  32,
	"c8g.12xlarge": 48,
	"c8g.16xlarge": 64,
}

// Direction is the role the connector-under-test plays in the pipeline.
// Source connectors read an external system and write into Redpanda (CDC);
// sink connectors read from Redpanda and write into an external system.
type Direction string

const (
	DirectionSource Direction = "source"
	DirectionSink   Direction = "sink"
)

type Scenario struct {
	Name        string         `yaml:"name"`
	Description string         `yaml:"description"`
	Connector   string         `yaml:"connector"`
	Direction   Direction      `yaml:"direction,omitempty"`
	Stack       string         `yaml:"stack"`
	Infra       InfraSpec      `yaml:"infra"`
	Dataset     DatasetSpec    `yaml:"dataset"`
	Workload    *WorkloadSpec  `yaml:"workload,omitempty"`
	Pipeline    map[string]any `yaml:"pipeline"`
	Matrix      MatrixSpec     `yaml:"matrix"`
	Reset       []ResetStep    `yaml:"reset"`
	// Soak marks this scenario as a long, single-CPU-point, sustained-moderate-
	// load run whose purpose is catching leaks/stalls/rotation bugs over wall
	// clock — unlike the short max-load sweeps matrix.cpu_points/arms are for.
	// Validate restricts a soak scenario to exactly one cpu_points entry and no
	// arms (see Validate).
	Soak bool `yaml:"soak,omitempty"`
}

type InfraSpec struct {
	Source map[string]any `yaml:"source"`
	Runner RunnerSpec     `yaml:"runner"`
}

type RunnerSpec struct {
	InstanceType string `yaml:"instance_type"`
}

type DatasetSpec struct {
	InitialRows       int64    `yaml:"initial_rows"`
	RowSizeBytes      int      `yaml:"row_size_bytes"`
	Tables            []string `yaml:"tables"`
	Seeder            string   `yaml:"seeder"`
	ExpectedPeakMBSec int      `yaml:"expected_peak_mb_s,omitempty"`
}

type WorkloadSpec struct {
	WriteRatePerSec int           `yaml:"write_rate_per_sec"`
	Duration        time.Duration `yaml:"duration"`
	Warmup          time.Duration `yaml:"warmup"`
}

type MatrixSpec struct {
	CPUPoints         []int                  `yaml:"cpu_points"`
	GoMemLimitPerVCPU int                    `yaml:"go_mem_limit_per_vcpu,omitempty"`
	Overrides         map[int]map[string]any `yaml:"overrides,omitempty"`
	// Arms turns a single cpu_points entry into an A/B: each arm is measured
	// at that same vCPU pin but with its own launch topology (GOMAXPROCS,
	// stream count) and pipeline overrides, or with a different binary (see
	// Arm.Binary). Empty for every pre-existing scenario, which keeps the
	// classic one-point-per-cpu_points behaviour.
	//
	// Streams > 1 (multiple launched pipelines per arm, each writing its own
	// per-stream-named resources) and fan_in (a single pipeline fanning N
	// pre-seeded topics into per-topic tables) were direction: sink-only
	// features; both return with the iceberg-sink stack PR. The remaining
	// source-direction shape — one pipeline per arm, distinguished by
	// gomaxprocs/pipeline overrides or by binary — is what
	// scenarios/postgres/orders-soak-pr.yaml uses today.
	Arms []Arm `yaml:"arms,omitempty"`
}

// Arm is one leg of an A/B at a fixed vCPU point. GOMAXPROCS may exceed the
// pinned core count deliberately: Connect counts licensed cores off the
// machine CPU rather than GOMAXPROCS, so oversubscribing is free.
type Arm struct {
	ID         string `yaml:"id"`
	GOMAXPROCS int    `yaml:"gomaxprocs,omitempty"`
	// Streams > 1 launches `redpanda-connect streams` with one config per
	// stream instead of `redpanda-connect run`. Validate rejects Streams > 1
	// for the only direction this build supports (source) — the per-stream
	// resource naming that made multi-stream meaningful was sink-only and
	// returns with the iceberg-sink stack PR.
	Streams int `yaml:"streams,omitempty"`
	// Pipeline is deep-merged over the scenario-level pipeline block for this
	// arm, so an arm declares only what differs. Applied to every stream.
	Pipeline map[string]any `yaml:"pipeline,omitempty"`
	// Binary names the LOGICAL binary this arm launches (e.g. "base", "pr"),
	// resolved at runtime to a staged path on the runner host — see
	// --binary in main.go and MatrixRunner.binaryPathFor in matrix.go. Empty
	// (the default for every pre-existing scenario) means the scenario's own
	// single staged binary, unchanged from before this field existed.
	//
	// This is the only shape a SOAK scenario's arms may take (see
	// Scenario.Validate): a base-vs-PR build comparison must hold everything
	// else — gomaxprocs, streams, pipeline — constant, or the two runs are
	// no longer an apples-to-apples comparison of the build alone.
	Binary string `yaml:"binary,omitempty"`
}

type ResetStep struct {
	SQL  string `yaml:"sql,omitempty"`
	Bash string `yaml:"bash,omitempty"`
}

// engineSpec captures the per-engine wiring needed to render seed/reset/workload
// scripts. Adding a new engine means adding a new entry in engineSpecs; no
// switch-statement edits anywhere else.
type engineSpec struct {
	// DSNOutputKey is the terraform output key holding the connection string.
	DSNOutputKey string
	// DSNEnvVar is the env var name to set in seed/workload scripts.
	DSNEnvVar string
	// For reset commands, the CLI tool may or may not accept a DSN URL. When
	// the engine's CLI does (e.g. psql), we leave the Reset*OutputKey fields
	// empty and the reset builder uses the DSN form. When it does not (e.g.
	// mysql, which wants discrete -h/-P/-u/-p flags), the Reset*OutputKey
	// fields point at terraform outputs and the reset builder uses those.
	ResetHostOutputKey string
	ResetPortOutputKey string
	ResetUserOutputKey string
	ResetPassOutputKey string
	ResetDBOutputKey   string

	// NoDSN, when true, indicates the engine doesn't use a DSN (e.g. IAM-authed
	// AWS services like DynamoDB). renderSeedScript and renderWorkloadScript
	// skip the DSN env-var prefix in this case. combineReset rejects `sql:`
	// reset steps for NoDSN engines — the scenario must use `bash:` steps.
	NoDSN bool
	// ExtraEnvVars maps an env-var name (e.g. "AWS_REGION") to a terraform
	// output key (e.g. "aws_region"). These are emitted as `KEY="value"`
	// prefixes on the seeder/workload commands in addition to (or instead of,
	// for NoDSN engines) the DSN env var. Keys are sorted before emission to
	// keep rendered scripts stable across Go's randomized map iteration.
	ExtraEnvVars map[string]string
}

// engineSpecs is the registry mechanism a connector's stack PR extends —
// see the type doc above. mysql_cdc, oracledb_cdc, microsoft_sql_server_cdc,
// mongodb_cdc, and aws_dynamodb_cdc were trimmed out of this scope-reduced
// tree (postgres_cdc soak testing only); each returns with its own stack PR.
var engineSpecs = map[string]engineSpec{
	"postgres_cdc": {
		DSNOutputKey: "postgres_dsn",
		DSNEnvVar:    "POSTGRES_DSN",
	},
}

func engineSpecFor(connector string) (engineSpec, bool) {
	es, ok := engineSpecs[connector]
	return es, ok
}

func LoadScenario(path string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var s Scenario
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	s.applyDirectionDefault()
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	if s.Matrix.GoMemLimitPerVCPU == 0 {
		s.Matrix.GoMemLimitPerVCPU = defaultGoMemPerVCPU
	}
	return &s, nil
}

// applyDirectionDefault sets the implicit "source" direction so existing CDC
// scenarios (which omit the field) keep their behavior.
func (s *Scenario) applyDirectionDefault() {
	if s.Direction == "" {
		s.Direction = DirectionSource
	}
}

func (s *Scenario) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if s.Connector == "" {
		return fmt.Errorf("connector is required")
	}
	switch s.Direction {
	case DirectionSource, DirectionSink, "":
		// "" is tolerated for direct struct construction in tests; LoadScenario
		// normalizes it via applyDirectionDefault.
	default:
		return fmt.Errorf("direction %q is invalid; must be %q or %q", s.Direction, DirectionSource, DirectionSink)
	}
	topo, err := topologyFor(s.Direction)
	if err != nil {
		return err
	}
	if err := topo.Validate(s); err != nil {
		return err
	}
	if s.Stack == "" {
		return fmt.Errorf("stack is required")
	}
	if len(s.Matrix.CPUPoints) == 0 {
		return fmt.Errorf("matrix.cpu_points must contain at least one value")
	}
	if s.Matrix.CPUPoints[0] < 1 {
		return fmt.Errorf("matrix.cpu_points must all be positive: %v", s.Matrix.CPUPoints)
	}
	for i := 1; i < len(s.Matrix.CPUPoints); i++ {
		if s.Matrix.CPUPoints[i] <= s.Matrix.CPUPoints[i-1] {
			return fmt.Errorf("matrix.cpu_points must be strictly ascending: %v", s.Matrix.CPUPoints)
		}
	}

	// A soak scenario measures ONE configuration held steady over a long wall
	// clock, so it can catch leaks/stalls/rotation bugs a short sweep never
	// runs long enough to hit. Sweeping cpu_points or A/B-ing arms is a
	// different question (find the ceiling / compare topologies) that the
	// bench profile already answers — checked here, before the general arms
	// validation below, so a soak+arms scenario gets this specific message
	// rather than getting tangled in arm-detail errors.
	//
	// The one exception (CON-179 R6 increment 5) is a base-vs-PR BUILD
	// comparison: running the identical soak configuration twice, once per
	// binary, is still "one configuration over time" — only the binary
	// launched differs. Arm.Binary is how a scenario opts into that shape,
	// and it is all-or-nothing: every arm must set a non-empty Binary, or a
	// mix of binary and non-binary arms would leave it ambiguous which arms
	// are even part of the comparison. Within that shape, no arm may also
	// override gomaxprocs/streams/fan_in/pipeline — a soak A/B must hold
	// everything constant except the build, or a throughput delta could be
	// explained by the override instead of the code change under test.
	if s.Soak {
		if len(s.Matrix.CPUPoints) != 1 {
			return fmt.Errorf("soak scenarios must set exactly one matrix.cpu_points entry (got %v): soak measures one configuration over time, not a sweep across configurations", s.Matrix.CPUPoints)
		}
		if len(s.Matrix.Arms) > 0 {
			allBinary := true
			for _, a := range s.Matrix.Arms {
				if a.Binary == "" {
					allBinary = false
					break
				}
			}
			if !allBinary {
				return fmt.Errorf("soak scenarios must not set matrix.arms unless every arm sets a non-empty binary (a base-vs-PR build comparison): soak measures one configuration over time, not an A/B comparison")
			}
			if len(s.Matrix.Arms) < 2 {
				return fmt.Errorf("soak scenarios using matrix.arms[].binary must set at least 2 arms (got %d): a build comparison needs two builds to compare", len(s.Matrix.Arms))
			}
			seenBinary := map[string]bool{}
			for i, a := range s.Matrix.Arms {
				if seenBinary[a.Binary] {
					return fmt.Errorf("matrix.arms[%d].binary %q is a duplicate; soak binary arms must have unique binary values", i, a.Binary)
				}
				seenBinary[a.Binary] = true
				if a.GOMAXPROCS != 0 || a.Streams != 0 || a.Pipeline != nil {
					return fmt.Errorf("matrix.arms[%d] (binary %q) must not override gomaxprocs/streams/pipeline: a soak A/B must hold everything constant except the build", i, a.Binary)
				}
			}
		}
	}

	if len(s.Matrix.Arms) > 0 {
		// By the time we get here, topologyFor above has already rejected
		// direction: sink, so s.Direction is always source. Source arms
		// render through the topology-agnostic renderPipelineConfig: one
		// pipeline per arm, arms differing by their pipeline override (e.g.
		// an oracledb_cdc arm mining one table vs. all five) or by binary
		// (the /soak base-vs-PR comparison). Streams > 1 stays rejected: its
		// renderer derives per-stream Iceberg table names, which has no
		// meaning for a CDC source and returns with the iceberg-sink stack
		// PR.
		for i, a := range s.Matrix.Arms {
			if a.Streams > 1 {
				return fmt.Errorf("matrix.arms[%d].streams must be <= 1 (got %d): multi-stream rendering derives per-stream Iceberg table names, which returns with the iceberg-sink stack PR", i, a.Streams)
			}
		}
		if product := len(s.Matrix.CPUPoints) * len(s.Matrix.Arms); product > maxArmSweepPoints {
			return fmt.Errorf("matrix.arms × matrix.cpu_points must expand to <= %d sweep points (got %d cpu_points × %d arms = %d): a careless scenario could otherwise commit to a day-long run",
				maxArmSweepPoints, len(s.Matrix.CPUPoints), len(s.Matrix.Arms), product)
		}
		seen := map[string]bool{}
		for i, a := range s.Matrix.Arms {
			if !armIDRe.MatchString(a.ID) {
				return fmt.Errorf("matrix.arms[%d].id %q must match %s (it is used in filenames and S3 keys)", i, a.ID, armIDRe.String())
			}
			if seen[a.ID] {
				return fmt.Errorf("matrix.arms[%d].id %q is a duplicate; arm ids must be unique", i, a.ID)
			}
			seen[a.ID] = true
			if a.GOMAXPROCS < 0 {
				return fmt.Errorf("matrix.arms[%d].gomaxprocs must be non-negative (got %d); 0 means use the default", i, a.GOMAXPROCS)
			}
			if a.GOMAXPROCS > maxArmGOMAXPROCS {
				return fmt.Errorf("matrix.arms[%d].gomaxprocs must be <= %d (got %d); the taskset core pin always follows vCPU, not GOMAXPROCS, so oversubscribing past the largest supported instance type's vCPU count buys nothing but scheduler contention", i, maxArmGOMAXPROCS, a.GOMAXPROCS)
			}
			if a.Streams < 0 {
				return fmt.Errorf("matrix.arms[%d].streams must be non-negative (got %d); 0 means use the default", i, a.Streams)
			}
		}
	}

	maxCPU := s.Matrix.CPUPoints[len(s.Matrix.CPUPoints)-1]
	vCPU := vcpuForInstanceType(s.Infra.Runner.InstanceType)
	if vCPU == 0 {
		return fmt.Errorf("infra.runner.instance_type %q: unknown vCPU count", s.Infra.Runner.InstanceType)
	}
	if vCPU < reservedCores+maxCPU {
		return fmt.Errorf("infra.runner.instance_type %q has %d vCPU but matrix requires %d (max sweep %d + %d reserved)",
			s.Infra.Runner.InstanceType, vCPU, reservedCores+maxCPU, maxCPU, reservedCores)
	}

	if s.Workload != nil {
		if s.Workload.Warmup < minWarmup {
			return fmt.Errorf("workload.warmup %s is below minimum %s", s.Workload.Warmup, minWarmup)
		}
		if s.Workload.Duration < minDuration {
			return fmt.Errorf("workload.duration %s is below minimum %s", s.Workload.Duration, minDuration)
		}
	} else {
		// Bounded-dataset scenario: require the size hint and verify wall-clock estimate.
		if s.Dataset.ExpectedPeakMBSec == 0 {
			return fmt.Errorf("bounded-dataset scenario must set dataset.expected_peak_mb_s")
		}
		totalBytes := s.Dataset.InitialRows * int64(s.Dataset.RowSizeBytes)
		mbTotal := totalBytes / bytesPerMB
		estSeconds := mbTotal / int64(s.Dataset.ExpectedPeakMBSec)
		if estSeconds < int64(minDuration.Seconds()) {
			return fmt.Errorf("bounded-dataset run would complete in %ds at %d MB/s — below minimum %s; increase dataset",
				estSeconds, s.Dataset.ExpectedPeakMBSec, minDuration)
		}
	}
	return nil
}

// IsBinaryArmScenario reports whether s.Matrix.Arms is in the soak
// base-vs-PR binary-arm shape Validate enforces: every arm sets a
// non-empty Binary. runBench uses this to gate the soak-index upload, the
// rolling-baseline comparator, and the base-vs-PR comparison markdown —
// all three only make sense in this shape (see main.go).
func (s *Scenario) IsBinaryArmScenario() bool {
	if len(s.Matrix.Arms) == 0 {
		return false
	}
	for _, a := range s.Matrix.Arms {
		if a.Binary == "" {
			return false
		}
	}
	return true
}

// vcpuForInstanceType returns the vCPU count for known instance types or 0 if
// unknown. Extend instanceTypeVCPU when new types are referenced by scenarios.
func vcpuForInstanceType(it string) int {
	return instanceTypeVCPU[it]
}
