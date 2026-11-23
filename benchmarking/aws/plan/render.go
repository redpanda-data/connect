// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
package plan

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultMessageSizeBytes is used when a plan's workload gives no size.
	DefaultMessageSizeBytes = 1024
	// DefaultPeakMBps is the assumed peak throughput a preload must outlast
	// when none is known.
	DefaultPeakMBps = 300.0

	// producerHeadroom oversizes the producer so it outpaces the pipeline.
	producerHeadroom = 1.5

	// memoryPerVCPUBytes is one vCPU's worth of CU memory (catalog sizing).
	memoryPerVCPUBytes = 4_000_000_000
)

// ProducerRatePerSec returns the messages per second a stack-none scenario's
// producer sustains: peakMBps (default DefaultPeakMBps when <= 0) with 50%
// headroom, divided by the message size (default DefaultMessageSizeBytes).
func ProducerRatePerSec(sizeBytes int, peakMBps float64) int {
	if peakMBps <= 0 {
		peakMBps = DefaultPeakMBps
	}
	if sizeBytes <= 0 {
		sizeBytes = DefaultMessageSizeBytes
	}
	return int(math.Ceil(peakMBps * 1e6 * producerHeadroom / float64(sizeBytes)))
}

type renderScenario struct {
	Name      string         `yaml:"name"`
	Direction string         `yaml:"direction"`
	Connector string         `yaml:"connector"`
	Stack     string         `yaml:"stack"`
	Infra     renderInfra    `yaml:"infra"`
	Dataset   renderDataset  `yaml:"dataset"`
	Workload  *renderLoad    `yaml:"workload,omitempty"`
	Pipeline  map[string]any `yaml:"pipeline"`
	Sink      renderSink     `yaml:"sink"`
	Matrix    renderMatrix   `yaml:"matrix"`
	Reset     []renderReset  `yaml:"reset,omitempty"`
	Catalog   renderCatalog  `yaml:"catalog"`
}

// renderCatalog is the plan's catalogue identity, carried to the runner so
// the entry names the customer's config rather than the identity-swapped
// pipeline the bench runs.
type renderCatalog struct {
	Fingerprint        string   `yaml:"fingerprint"`
	Input              string   `yaml:"input"`
	Processors         []string `yaml:"processors"`
	Output             string   `yaml:"output"`
	Caveats            []string `yaml:"caveats"`
	SourceApproximated bool     `yaml:"source_approximated"`
}

type renderInfra struct {
	Source             map[string]any `yaml:"source,omitempty"`
	Runner             renderRunner   `yaml:"runner"`
	MemoryPerVCPUBytes int64          `yaml:"memory_per_vcpu_bytes"`
}

type renderRunner struct {
	InstanceType string `yaml:"instance_type"`
}

type renderDataset struct {
	InitialRows  *int64   `yaml:"initial_rows,omitempty"`
	RowSizeBytes int      `yaml:"row_size_bytes"`
	Tables       []string `yaml:"tables,omitempty"`
	Seeder       string   `yaml:"seeder,omitempty"`
}

type renderLoad struct {
	WriteRatePerSec int    `yaml:"write_rate_per_sec"`
	Duration        string `yaml:"duration"`
	Warmup          string `yaml:"warmup"`
}

type renderSink struct {
	Kind string `yaml:"kind"`
	Size string `yaml:"size,omitempty"`
}

type renderMatrix struct {
	CPUPoints []int `yaml:"cpu_points"`
}

type renderReset struct {
	SQL string `yaml:"sql"`
}

// Base settings per stack. These are Go literals, deliberately not read from
// the repo's scenario files at runtime; they mirror
// scenarios/postgres/orders-cdc.yaml and scenarios/mysql/orders-soak.yaml.

func postgresBase() (infraSource map[string]any, runner string, ds renderDataset, load *renderLoad, reset []renderReset) {
	zero := int64(0)
	return map[string]any{
			"instance_class":     "db.r6g.4xlarge",
			"storage_gb":         800,
			"iops":               24000,
			"storage_throughput": 1000,
			"parameters": map[string]any{
				"rds.logical_replication": "1",
				"max_wal_senders":         "20",
			},
		}, "c8g.4xlarge",
		renderDataset{InitialRows: &zero, Tables: []string{"orders"}, Seeder: "cdc-rows-postgres"},
		&renderLoad{WriteRatePerSec: 150000, Duration: "15m", Warmup: "2m"},
		[]renderReset{
			{SQL: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"},
			{SQL: "TRUNCATE TABLE orders"},
		}
}

func mysqlBase() (infraSource map[string]any, runner string, ds renderDataset, load *renderLoad, reset []renderReset) {
	zero := int64(0)
	return map[string]any{
			"instance_class":     "db.r6g.xlarge",
			"storage_gb":         400,
			"iops":               12000,
			"storage_throughput": 500,
			"parameters": map[string]any{
				"binlog_format":    "ROW",
				"binlog_row_image": "FULL",
				"binlog_checksum":  "NONE",
			},
		},
		// The soak's c8g.xlarge (4 vCPU) cannot host the 1,2,4,8 sweep, which
		// needs 8 + 2 reserved vCPU.
		"c8g.4xlarge",
		renderDataset{InitialRows: &zero, Tables: []string{"orders"}, Seeder: "cdc-rows-mysql"},
		&renderLoad{WriteRatePerSec: 150000, Duration: "15m", Warmup: "2m"},
		[]renderReset{
			{SQL: "CALL mysql.rds_set_configuration('binlog retention hours', 24)"},
			{SQL: "TRUNCATE TABLE orders"},
		}
}

// Render turns a runnable Plan into a runner scenario YAML (direction: e2e).
func Render(p Plan) ([]byte, error) {
	if !p.Runnable {
		reason := p.Reason
		if reason == "" {
			reason = "plan is not runnable"
		}
		return nil, errors.New(reason)
	}
	size := p.Workload.MessageSizeBytes
	sizeComment := "from the plan workload"
	if size <= 0 {
		size, sizeComment = DefaultMessageSizeBytes, "default; the plan gave no message size"
	}

	sc := renderScenario{
		Name:      "bot-" + p.ID,
		Direction: "e2e",
		Connector: p.Input,
		Pipeline:  p.Pipeline,
		Sink:      renderSink{Kind: string(p.Sink), Size: p.SinkSize},
		Matrix:    renderMatrix{CPUPoints: []int{1, 2, 4, 8}},
		Catalog: renderCatalog{
			Fingerprint:        p.Fingerprint,
			Input:              p.Input,
			Processors:         nonNil(p.Processors),
			Output:             p.Output,
			Caveats:            nonNil(p.Caveats),
			SourceApproximated: p.Source == SourceApproximated,
		},
	}
	sc.Infra.MemoryPerVCPUBytes = memoryPerVCPUBytes
	var startingValue bool
	sc.Stack = stackFor(p.Source)
	switch p.Source {
	case SourcePostgres:
		sc.Infra.Source, sc.Infra.Runner.InstanceType, sc.Dataset, sc.Workload, sc.Reset = postgresBase()
	case SourceMySQL:
		sc.Infra.Source, sc.Infra.Runner.InstanceType, sc.Dataset, sc.Workload, sc.Reset = mysqlBase()
		startingValue = true
	default:
		// The sweep consumes as fast as the pipeline allows, so the target
		// rate is no bound on consumption: size for the default peak.
		if p.Source == SourceApproximated {
			sc.Pipeline = approximatedPipeline(p.Pipeline)
		}
		sc.Infra.Runner.InstanceType = "c8g.4xlarge"
		sc.Workload = &renderLoad{
			WriteRatePerSec: ProducerRatePerSec(size, 0),
			Duration:        "15m",
			Warmup:          "2m",
		}
	}
	sc.Dataset.RowSizeBytes = size

	var root yaml.Node
	if err := root.Encode(sc); err != nil {
		return nil, fmt.Errorf("encoding scenario: %w", err)
	}
	comment(&root, "row_size_bytes", sizeComment, "dataset")
	if startingValue {
		comment(&root, "write_rate_per_sec", "starting value, copied from the postgres bench; not tuned for mysql", "workload")
	}
	out, err := yaml.Marshal(&root)
	if err != nil {
		return nil, fmt.Errorf("marshalling scenario: %w", err)
	}
	return out, nil
}

// lintPlaceholder matches the bench placeholders the runner fills at run
// time: ${BENCH_*}, ${POSTGRES_DSN} and ${MYSQL_DSN}.
var lintPlaceholder = regexp.MustCompile(`\$\{(BENCH_[A-Z0-9_]*|POSTGRES_DSN|MYSQL_DSN)\}`)

// lintDummies are syntactically valid stand-ins for placeholders that are
// not plain names; every other placeholder becomes "bench".
var lintDummies = map[string]string{
	"POSTGRES_DSN":                     "postgres://bench:bench@localhost:5432/bench",
	"MYSQL_DSN":                        "bench:bench@tcp(localhost:3306)/bench",
	"BENCH_REDPANDA_BROKERS":           "localhost:9092",
	"BENCH_AWS_REGION":                 "us-east-2",
	"BENCH_ICEBERG_CATALOG_URL":        "http://localhost:8181",
	"BENCH_ICEBERG_TABLE_LOCATION":     "s3://bench/bench",
	"BENCH_SNOWFLAKE_PRIVATE_KEY_FILE": "/tmp/bench.p8",
	"BENCH_S3_PREFIX":                  "bench/",
}

// ConnectConfig renders the Connect config the bench will run for p (the
// scenario's pipeline, as Render writes it) as a standalone YAML document,
// with every bench placeholder replaced by a syntactically valid dummy, so
// `redpanda-connect lint` can check it before any spend. It is the
// customer's pipeline logic: never print it in a public log.
func ConnectConfig(p Plan) ([]byte, error) {
	if !p.Runnable {
		return nil, errors.New("plan is not runnable")
	}
	pipeline := p.Pipeline
	if p.Source == SourceApproximated {
		pipeline = approximatedPipeline(p.Pipeline)
	}
	cfg, _ := fillLintDummies(deepCopy(pipeline)).(map[string]any)
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshalling connect config: %w", err)
	}
	return out, nil
}

func fillLintDummies(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, c := range t {
			t[k] = fillLintDummies(c)
		}
	case []any:
		for i, c := range t {
			t[i] = fillLintDummies(c)
		}
	case string:
		return lintPlaceholder.ReplaceAllStringFunc(t, func(m string) string {
			if d, ok := lintDummies[m[2:len(m)-1]]; ok {
				return d
			}
			return "bench"
		})
	}
	return v
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// approximatedPipeline returns a copy of pipeline whose input is the bench's
// redpanda input reading the source topic the producer fills. An approximated
// input (http_server, generate, ...) cannot run as written: SwapIdentity has
// stripped its strings, and the bench has no stack for it. Input-level
// processors are pipeline logic and are kept; the stored Plan is not changed.
func approximatedPipeline(pipeline map[string]any) map[string]any {
	out := make(map[string]any, len(pipeline))
	maps.Copy(out, pipeline)
	const component = "redpanda"
	tpl := SourceTemplates[SourceRedpanda](component)
	in := map[string]any{component: tpl.Fields}
	if orig, ok := pipeline["input"].(map[string]any); ok {
		if procs, ok := orig["processors"]; ok {
			in["processors"] = deepCopy(procs)
		}
	}
	out["input"] = in
	return out
}

// comment sets a line comment on the value of key found inside the mapping
// named parent (a top-level key).
func comment(root *yaml.Node, key, text, parent string) {
	m := root
	if m.Kind == yaml.DocumentNode && len(m.Content) > 0 {
		m = m.Content[0]
	}
	child := mapValue(m, parent)
	if child == nil {
		return
	}
	for i := 0; i+1 < len(child.Content); i += 2 {
		if child.Content[i].Value == key {
			child.Content[i+1].LineComment = text
		}
	}
}

func mapValue(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}
