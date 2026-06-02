// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	minWarmup           = 2 * time.Minute
	minDuration         = 15 * time.Minute
	reservedCores       = 2
	defaultGoMemPerVCPU = 2 // GiB
)

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
	// KafkaConnect is an optional override map applied on top of the
	// kcConnectorSpec registry entry's PropsTemplate at render time. The
	// fields here are shallow-merged into the resulting KC connector config
	// JSON. Use this to tune e.g. snapshot.mode without editing the registry.
	KafkaConnect map[string]any `yaml:"kafka_connect,omitempty"`
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

var engineSpecs = map[string]engineSpec{
	"postgres_cdc": {
		DSNOutputKey: "postgres_dsn",
		DSNEnvVar:    "POSTGRES_DSN",
	},
	"mysql_cdc": {
		DSNOutputKey:       "mysql_dsn",
		DSNEnvVar:          "MYSQL_DSN",
		ResetHostOutputKey: "mysql_host",
		ResetPortOutputKey: "mysql_port",
		ResetUserOutputKey: "mysql_user",
		ResetPassOutputKey: "mysql_password",
		ResetDBOutputKey:   "mysql_db",
	},
	// aws_dynamodb_cdc uses IAM auth (no DSN). The seeder reads AWS_REGION and
	// DDB_TABLE from its env, and the bash reset steps reference them via
	// ${AWS_REGION} / ${DYNAMODB_TABLE_NAME} placeholders. No KC counterpart —
	// Debezium 2.7.x doesn't ship a DynamoDB connector and the bench cloud-init
	// doesn't install a paid alternative, so this scenario only runs against
	// --engines=connect.
	"aws_dynamodb_cdc": {
		NoDSN: true,
		ExtraEnvVars: map[string]string{
			"AWS_REGION": "aws_region",
			"DDB_TABLE":  "dynamodb_table_name",
		},
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
		mbTotal := totalBytes / (1024 * 1024)
		estSeconds := mbTotal / int64(s.Dataset.ExpectedPeakMBSec)
		if estSeconds < int64(minDuration.Seconds()) {
			return fmt.Errorf("bounded-dataset run would complete in %ds at %d MB/s — below minimum %s; increase dataset",
				estSeconds, s.Dataset.ExpectedPeakMBSec, minDuration)
		}
	}
	return nil
}

// vcpuForInstanceType returns the vCPU count for known instance types or 0 if
// unknown. Extend instanceTypeVCPU when new types are referenced by scenarios.
func vcpuForInstanceType(it string) int {
	return instanceTypeVCPU[it]
}
