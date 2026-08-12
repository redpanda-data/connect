// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

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
	// maxArmStreams bounds Arm.Streams. Each stream adds one rendered config,
	// one Iceberg table, and one retried iceberg-tablegen pre-create PER
	// ENGINE at every between-points reset (see IcebergResetTables) — a
	// typo like `streams: 1000` would render ~1001 tables in the reset
	// union and ~2002 tablegen invocations per reset, all against real AWS
	// spend. 8 is generous headroom over the plan's own 2-stream arm B.
	maxArmStreams = 8
	// maxDatasetTopics bounds DatasetSpec.Topics. Each topic adds one seeder
	// invocation, one Iceberg table, and (in streams mode) one consumer
	// group — 16 mirrors the generous headroom maxArmStreams already gives
	// per-stream resources, well above the 7-topic case this exists for.
	maxDatasetTopics = 16
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
	// KafkaConnect is an optional override map applied on top of the
	// kcConnectorSpec registry entry's PropsTemplate at render time. The
	// fields here are shallow-merged into the resulting KC connector config
	// JSON. Use this to tune e.g. snapshot.mode without editing the registry.
	KafkaConnect map[string]any `yaml:"kafka_connect,omitempty"`
	// Soak marks this scenario as a long, single-CPU-point, sustained-moderate-
	// load run whose purpose is catching leaks/stalls/rotation bugs over wall
	// clock — unlike the short max-load sweeps matrix.cpu_points/arms are for.
	// Validate restricts a soak scenario to exactly one cpu_points entry and no
	// arms (see Validate); runBench additionally requires --engines=connect
	// (checked there, not here, since engines is a CLI flag rather than a
	// scenario field).
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
	// Topics splits InitialRows evenly across N pre-seeded source topics
	// instead of one. 0 (absent) and 1 both mean single-topic, which keeps
	// every existing scenario byte-identical: same topic name, same table
	// name, same consumer group, same seed script. Sink-only (see Validate).
	// See BenchNames.WithTopics/WithTopic for the corresponding naming.
	Topics int `yaml:"topics,omitempty"`
	// PartitionsPerTopic is the per-topic partition count the seeder
	// pre-creates when Topics > 1. Ignored when Topics <= 1 — the seeder's
	// own --partitions default (16) applies instead, unchanged. See
	// partitionsPerTopic for the Topics > 1 default (4).
	PartitionsPerTopic int `yaml:"partitions_per_topic,omitempty"`
}

// partitionsPerTopic is the effective per-topic partition count to seed with
// when Topics > 1, defaulting to 4 (7 topics x 4 = 28 partitions, ample for
// <=4 cores and far faster to seed than reusing the single-topic default of
// 16 per topic).
func (d DatasetSpec) partitionsPerTopic() int {
	if d.PartitionsPerTopic > 0 {
		return d.PartitionsPerTopic
	}
	return 4
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
	// stream count) and pipeline overrides. Sink-only, Connect-only. Empty
	// for every pre-existing scenario, which keeps the classic
	// one-point-per-cpu_points behaviour.
	Arms []Arm `yaml:"arms,omitempty"`
}

// Arm is one leg of an A/B at a fixed vCPU point. GOMAXPROCS may exceed the
// pinned core count deliberately: Connect counts licensed cores off the machine
// CPU rather than GOMAXPROCS, so oversubscribing is free, and an I/O-blocked
// output (e.g. iceberg commits) can leave pinned cores idle without it.
// Streams > 1 launches `redpanda-connect streams` with one config per stream
// instead of `redpanda-connect run`.
type Arm struct {
	ID         string `yaml:"id"`
	GOMAXPROCS int    `yaml:"gomaxprocs,omitempty"`
	Streams    int    `yaml:"streams,omitempty"`
	// FanIn true renders this arm as ONE pipeline subscribed to all of
	// dataset.topics' N topics, routing each record to its topic-derived
	// Iceberg table via an interpolated `table` field (see fanInTableExpr in
	// main.go) instead of streams mode's one-stream-per-topic default.
	// Requires dataset.topics > 1 (fanning a single topic in is meaningless)
	// and is mutually exclusive with Streams > 1 (fan-in IS a single
	// pipeline) — Validate enforces both.
	FanIn bool `yaml:"fan_in,omitempty"`
	// Pipeline is deep-merged over the scenario-level pipeline block for this
	// arm, so an arm declares only what differs. Applied to every stream.
	Pipeline map[string]any `yaml:"pipeline,omitempty"`
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
	// oracledb_cdc connects via a go-ora DSN URL (connection_string). Oracle has
	// no psql/mysql CLI on the runner, so the scenario's reset is a bash: step
	// that shells out to the cdc-rows-oracle seeder's `exec` subcommand. The
	// discrete Reset*OutputKey fields here drive the KC (Debezium Oracle) render
	// in main.go, which needs host/port/user/password/dbname split out.
	"oracledb_cdc": {
		DSNOutputKey:       "oracle_dsn",
		DSNEnvVar:          "ORACLE_DSN",
		ResetHostOutputKey: "oracle_host",
		ResetPortOutputKey: "oracle_port",
		ResetUserOutputKey: "oracle_user",
		ResetPassOutputKey: "oracle_password",
		ResetDBOutputKey:   "oracle_db",
	},
	// microsoft_sql_server_cdc connects via a go-mssqldb DSN URL
	// (connection_string). The discrete Reset*OutputKey fields drive the KC
	// (Debezium SQL Server) render in main.go, which needs host/port/user/
	// password/dbname split out.
	//
	// MSSQL_MASTER_DSN is the SQL-Server-only wrinkle: RDS rejects `db_name` for
	// every sqlserver engine, so the instance comes up with only its system
	// databases and MSSQL_DSN isn't connectable until something CREATEs the
	// application database. The seeder does that against the master DSN first,
	// which is also where database-level CDC gets enabled (via
	// msdb.dbo.rds_cdc_enable_db — the native sys.sp_cdc_enable_db needs
	// sysadmin, which RDS does not grant).
	//
	// There is no sqlcmd on the runner, so the scenario's reset is a bash: step
	// shelling out to the cdc-rows-mssql seeder's `reset` subcommand.
	"microsoft_sql_server_cdc": {
		DSNOutputKey:       "mssql_dsn",
		DSNEnvVar:          "MSSQL_DSN",
		ResetHostOutputKey: "mssql_host",
		ResetPortOutputKey: "mssql_port",
		ResetUserOutputKey: "mssql_user",
		ResetPassOutputKey: "mssql_password",
		ResetDBOutputKey:   "mssql_db",
		ExtraEnvVars: map[string]string{
			"MSSQL_MASTER_DSN": "mssql_master_dsn",
		},
	},
	// mongodb_cdc streams MongoDB change streams from a self-hosted single-node
	// replica set (terraform modules/mongodb-ec2). mongod runs without auth, so
	// the mongodb_user / mongodb_password terraform outputs are empty strings and
	// the KC connection-string template omits credentials; the discrete
	// Reset*OutputKey fields still feed Host/Port into buildKCRenderInputs. There
	// is no mongosh on the runner, so the scenario's reset is a bash: step that
	// shells out to the cdc-rows-mongodb seeder's `exec` subcommand.
	"mongodb_cdc": {
		DSNOutputKey:       "mongodb_dsn",
		DSNEnvVar:          "MONGODB_DSN",
		ResetHostOutputKey: "mongodb_host",
		ResetPortOutputKey: "mongodb_port",
		ResetUserOutputKey: "mongodb_user",
		ResetPassOutputKey: "mongodb_password",
		ResetDBOutputKey:   "mongodb_db",
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
			"AWS_REGION":     "aws_region",
			"DDB_TABLE":      "dynamodb_table_name",
			"READ_CAPACITY":  "read_capacity",
			"WRITE_CAPACITY": "write_capacity",
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

	// A soak scenario measures ONE configuration held steady over a long wall
	// clock, so it can catch leaks/stalls/rotation bugs a short sweep never
	// runs long enough to hit. Sweeping cpu_points or A/B-ing arms is a
	// different question (find the ceiling / compare topologies) that the
	// bench profile already answers — checked here, before the general arms
	// validation below, so a soak+arms scenario gets this specific message
	// rather than getting tangled in arm-detail errors.
	if s.Soak {
		if len(s.Matrix.CPUPoints) != 1 {
			return fmt.Errorf("soak scenarios must set exactly one matrix.cpu_points entry (got %v): soak measures one configuration over time, not a sweep across configurations", s.Matrix.CPUPoints)
		}
		if len(s.Matrix.Arms) > 0 {
			return fmt.Errorf("soak scenarios must not set matrix.arms: soak measures one configuration over time, not an A/B comparison")
		}
	}

	if len(s.Matrix.Arms) > 0 {
		// Source scenarios may use arms, but only in the shape that renders
		// through the topology-agnostic renderPipelineConfig: one pipeline per
		// arm, arms differing by their pipeline override (e.g. an oracledb_cdc
		// arm mining one table vs. all five, on the same RDS instance and the
		// same load — the whole point of an arm rather than two separate runs).
		//
		// fan_in and streams > 1 stay sink-only because their renderers are
		// sink-shaped: renderFanInConfig requires a redpanda input plus a
		// table-bearing output, and multi-stream rendering derives per-topic
		// names and Iceberg tables. Neither has any meaning for a CDC source.
		if s.Direction != DirectionSink {
			for i, a := range s.Matrix.Arms {
				if a.FanIn {
					return fmt.Errorf("matrix.arms[%d].fan_in is only supported for direction: sink (got %q); fan-in renders a redpanda input into a table-bearing output", i, s.Direction)
				}
				if a.Streams > 1 {
					return fmt.Errorf("matrix.arms[%d].streams must be <= 1 for direction: %q (got %d); multi-stream rendering derives per-topic names and sink tables", i, s.Direction, a.Streams)
				}
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
			if a.Streams > maxArmStreams {
				return fmt.Errorf("matrix.arms[%d].streams must be <= %d (got %d); each stream adds a rendered config, an Iceberg table, and a retried tablegen pre-create per engine at every between-points reset", i, maxArmStreams, a.Streams)
			}
			if a.FanIn && s.Dataset.Topics <= 1 {
				return fmt.Errorf("matrix.arms[%d].fan_in requires dataset.topics > 1 (got %d); fanning a single topic in is meaningless", i, s.Dataset.Topics)
			}
			if a.FanIn && a.Streams > 1 {
				return fmt.Errorf("matrix.arms[%d].fan_in is mutually exclusive with streams > 1 (got streams: %d); fan-in is a single pipeline", i, a.Streams)
			}
		}
	}

	if s.Dataset.Topics > 1 {
		if s.Direction != DirectionSink {
			return fmt.Errorf("dataset.topics > 1 is only supported for direction: sink (got %q)", s.Direction)
		}
		if s.Dataset.Topics > maxDatasetTopics {
			return fmt.Errorf("dataset.topics must be <= %d (got %d)", maxDatasetTopics, s.Dataset.Topics)
		}
		if s.Dataset.InitialRows%int64(s.Dataset.Topics) != 0 {
			return fmt.Errorf("dataset.initial_rows (%d) must be evenly divisible by dataset.topics (%d) so the per-topic split is exact",
				s.Dataset.InitialRows, s.Dataset.Topics)
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

// vcpuForInstanceType returns the vCPU count for known instance types or 0 if
// unknown. Extend instanceTypeVCPU when new types are referenced by scenarios.
func vcpuForInstanceType(it string) int {
	return instanceTypeVCPU[it]
}
