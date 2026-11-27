package plan

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var update = flag.Bool("update", false, "rewrite golden files")

func httpToDrop() map[string]any {
	return map[string]any{
		"input":  map[string]any{"http_server": map[string]any{"path": "/post"}},
		"output": map[string]any{"drop": map[string]any{}},
	}
}

func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func TestRenderPostgresToSnowflakeGolden(t *testing.T) {
	p := build(t, pgToSnowflake())
	p.ID = "aaaaaaaaaaaa"
	out, err := Render(p)
	require.NoError(t, err)
	checkGolden(t, "bot-aaaaaaaaaaaa.yaml", out)
}

func TestRenderApproximatedToDropGolden(t *testing.T) {
	p := build(t, httpToDrop())
	p.ID = "bbbbbbbbbbbb"
	require.Equal(t, SourceApproximated, p.Source)
	out, err := Render(p)
	require.NoError(t, err)
	checkGolden(t, "bot-bbbbbbbbbbbb.yaml", out)
}

func TestRenderRejectsUnrunnable(t *testing.T) {
	_, err := Render(Plan{ID: "x", Runnable: false, Reason: "no bench stack for oracledb_cdc yet"})
	require.ErrorContains(t, err, "no bench stack for oracledb_cdc yet")
}

func TestRenderDefaultsRowSize(t *testing.T) {
	p := build(t, httpToDrop())
	p.Workload.MessageSizeBytes = 0
	out, err := Render(p)
	require.NoError(t, err)
	require.Contains(t, string(out), "row_size_bytes: 1024")
}

func TestRenderMySQLUsesMySQLBase(t *testing.T) {
	p := build(t, pgToSnowflake())
	p.Source, p.Input = SourceMySQL, "mysql_cdc"
	out, err := Render(p)
	require.NoError(t, err)
	s := string(out)
	require.Contains(t, s, "stack: mysql")
	require.Contains(t, s, "cdc-rows-mysql")
	require.Contains(t, s, "binlog retention hours")
	require.Contains(t, s, "instance_type: c8g.4xlarge")
}

func TestProducerRatePerSec(t *testing.T) {
	// 300 MB/s * 1.5 / 1000 B = 450,000 msg/s
	require.Equal(t, 450_000, ProducerRatePerSec(1000, 300))
	require.Equal(t, 450_000, ProducerRatePerSec(1000, 0), "peak defaults to 300")
	require.Equal(t, 439_454, ProducerRatePerSec(1024, 0), "size 0 defaults to 1024 elsewhere; ceil here")
	require.Equal(t, 439_454, ProducerRatePerSec(0, 0))
}

func TestRenderMySQLToRedpandaGolden(t *testing.T) {
	cfg := map[string]any{
		"input": map[string]any{"mysql_cdc": map[string]any{
			"dsn":    "user:pw@tcp(db.customer.internal:3306)/prod",
			"tables": []any{"customers"},
		}},
		"output": map[string]any{"kafka_franz": map[string]any{
			"seed_brokers": []any{"broker.customer.internal:9092"}, "topic": "out",
		}},
	}
	p := build(t, cfg)
	p.ID = "cccccccccccc"
	require.Equal(t, SourceMySQL, p.Source)
	out, err := Render(p)
	require.NoError(t, err)
	require.Contains(t, string(out), "bench_cache")
	require.Contains(t, string(out), "checkpoint_limit: 10000")
	checkGolden(t, "bot-cccccccccccc.yaml", out)
}

func TestRenderApproximatedInputReadsBenchTopic(t *testing.T) {
	cfg := map[string]any{
		"input": map[string]any{
			"http_server": map[string]any{"path": "/post", "timeout": "5s"},
			"processors":  []any{map[string]any{"mapping": "root = this"}},
		},
		"output": map[string]any{"drop": map[string]any{}},
	}
	p := build(t, cfg)
	require.Equal(t, SourceApproximated, p.Source)
	out, err := Render(p)
	require.NoError(t, err)

	var sc struct {
		Connector string         `yaml:"connector"`
		Pipeline  map[string]any `yaml:"pipeline"`
	}
	require.NoError(t, yaml.Unmarshal(out, &sc))
	require.Equal(t, "http_server", sc.Connector, "the catalogue still names the customer's input")
	in := sc.Pipeline["input"].(map[string]any)
	require.NotContains(t, in, "http_server")
	require.Equal(t, map[string]any{
		"seed_brokers":   []any{"${BENCH_REDPANDA_BROKERS}"},
		"topics":         []any{"${BENCH_TOPIC}"},
		"consumer_group": "${BENCH_CONSUMER_GROUP}",
		"start_offset":   "earliest",
	}, in["redpanda"])
	require.Equal(t, []any{map[string]any{"mapping": "root = this"}}, in["processors"],
		"input-level processors are pipeline logic and stay")
	// The stored plan is untouched: the swap happens at render time only.
	require.Contains(t, p.Pipeline["input"], "http_server")
}

// The rendered scenario carries the plan's catalogue identity: the
// fingerprint of the customer's original config and its component names.
func TestRenderCatalogBlock(t *testing.T) {
	p := build(t, httpToDrop())
	out, err := Render(p)
	require.NoError(t, err)
	var sc struct {
		Catalog struct {
			Fingerprint        string   `yaml:"fingerprint"`
			Input              string   `yaml:"input"`
			Processors         []string `yaml:"processors"`
			Output             string   `yaml:"output"`
			Caveats            []string `yaml:"caveats"`
			SourceApproximated bool     `yaml:"source_approximated"`
		} `yaml:"catalog"`
	}
	require.NoError(t, yaml.Unmarshal(out, &sc))
	require.Equal(t, p.Fingerprint, sc.Catalog.Fingerprint)
	require.Equal(t, "http_server", sc.Catalog.Input)
	require.Empty(t, sc.Catalog.Processors)
	require.Equal(t, "drop", sc.Catalog.Output)
	require.Equal(t, p.Caveats, sc.Catalog.Caveats)
	require.True(t, sc.Catalog.SourceApproximated)
}

func TestConnectConfigReplacesBenchPlaceholders(t *testing.T) {
	cfg := pgToSnowflake()
	cfg["pipeline"] = map[string]any{"processors": []any{map[string]any{"mapping": `root.id = "${!counter()}"`}}}
	p := build(t, cfg)
	out, err := ConnectConfig(p)
	require.NoError(t, err)
	require.NotRegexp(t, `\$\{(BENCH_|POSTGRES_DSN|MYSQL_DSN)`, string(out))
	require.Contains(t, string(out), "postgres://bench:bench@localhost:5432/bench")
	require.Contains(t, string(out), "/tmp/bench.p8")
	require.Contains(t, string(out), `${!counter()}`, "Bloblang interpolations are kept")
	require.NotContains(t, string(out), "catalog:", "a Connect config, not a scenario")

	// The stored plan is not changed.
	require.Equal(t, "${POSTGRES_DSN}", p.Pipeline["input"].(map[string]any)["postgres_cdc"].(map[string]any)["dsn"])

	approx := build(t, httpToDrop())
	out, err = ConnectConfig(approx)
	require.NoError(t, err)
	require.Contains(t, string(out), "redpanda:", "the approximated input is the bench's redpanda input")
	require.Contains(t, string(out), "localhost:9092")

	_, err = ConnectConfig(Plan{Reason: "x"})
	require.Error(t, err)
}
