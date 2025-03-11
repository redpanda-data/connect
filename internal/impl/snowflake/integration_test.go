/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package snowflake_test

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/bits"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	_ "github.com/snowflakedb/gosnowflake"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	_ "github.com/redpanda-data/connect/v4/internal/impl/sql"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func EnvOrDefault(name, fallback string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		value = fallback
	}
	return value
}

// Global config is helpful to make the tests a bit more readable.
var config struct {
	db             string
	schema         string
	account        string
	role           string
	user           string
	privateKeyFile string
	privateKey     string
	dsn            string
}

func ReplaceConfig(s string) string {
	return strings.NewReplacer(
		"$USER", config.user,
		"$ACCOUNT", config.account,
		"$DB", config.db,
		"$ROLE", config.role,
		"$SCHEMA", config.schema,
		"$PRIVATE_KEY_FILE", config.privateKeyFile,
		"$PRIVATE_KEY", config.privateKey,
		"$DSN", config.dsn,
	).Replace(s)
}

func SetupConfig() {
	config.account = EnvOrDefault("SNOWFLAKE_ACCOUNT", "WQKFXQQ-WI77362")
	config.user = EnvOrDefault("SNOWFLAKE_USER", "ROCKWOODREDPANDA")
	config.db = EnvOrDefault("SNOWFLAKE_DB", "BABY_DATABASE")
	config.role = EnvOrDefault("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
	config.schema = EnvOrDefault("SNOWFLAKE_SCHEMA", "PUBLIC")
	config.privateKeyFile = EnvOrDefault("SNOWFLAKE_PRIVATE_KEY", "./streaming/resources/rsa_key.p8")
	bytes, err := os.ReadFile(config.privateKeyFile)
	if err != nil {
		panic(err)
	}
	privateKeyBlock, _ := pem.Decode(bytes)
	if privateKeyBlock == nil {
		panic("invalid private key file")
	}
	config.privateKey = base64.URLEncoding.EncodeToString(privateKeyBlock.Bytes)
	config.dsn = ReplaceConfig(
		"$USER@$ACCOUNT.snowflakecomputing.com/$DB/$SCHEMA?role=$ROLE&warehouse=compute_wh&authenticator=snowflake_jwt&privateKey=$PRIVATE_KEY",
	)
}

func Batch(rows []map[string]any) service.MessageBatch {
	var batch service.MessageBatch
	for _, row := range rows {
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(row)
		batch = append(batch, msg)
	}
	return batch
}

func SetupSnowflakeStream(t *testing.T, outputConfiguration string) (func([]map[string]any) error, *service.Stream) {
	SetupConfig()
	t.Helper()
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	produce, err := streamBuilder.AddBatchProducerFunc()
	require.NoError(t, err)
	require.NoError(t, streamBuilder.AddOutputYAML(ReplaceConfig(outputConfiguration)))
	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	t.Cleanup(func() {
		err := stream.Stop(context.Background())
		require.NoError(t, err)
	})
	return func(b []map[string]any) error {
		return produce(context.Background(), Batch(b))
	}, stream
}

func RunStreamInBackground(t *testing.T, stream *service.Stream) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error("failed to run stream: ", err)
		}
		wg.Done()
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

func RunSQLQuery(t *testing.T, stream *service.Stream, sql string) [][]string {
	t.Helper()
	resource, ok := stream.Resources().GetGeneric(snowflake.SnowflakeClientResourceForTesting)
	require.True(t, ok)
	client, ok := resource.(*streaming.SnowflakeRestClient)
	require.True(t, ok)
	resp, err := client.RunSQL(context.Background(), streaming.RunSQLRequest{
		Statement: ReplaceConfig(sql),
		Database:  config.db,
		Schema:    config.schema,
		Role:      config.role,
		Timeout:   30,
		Parameters: map[string]string{
			"TIMESTAMP_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
			"TIME_OUTPUT_FORMAT":      "HH24:MI:SS",
			"DATE_OUTPUT_FORMAT":      "YYYY-MM-DD",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "00000", resp.SQLState)
	return resp.Data
}

func TestIntegrationExactlyOnceDelivery(t *testing.T) {
	integration.CheckSkip(t)
	produce, stream := SetupSnowflakeStream(t, `
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_exactly_once
  init_statement: |
    DROP TABLE IF EXISTS integration_test_exactly_once;
  max_in_flight: 1
  offset_token: "${!this.token}"
  schema_evolution:
    enabled: true
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce([]map[string]any{
		{"foo": "bar", "token": 1},
		{"foo": "baz", "token": 2},
		{"foo": "qux", "token": 3},
		{"foo": "zoom", "token": 4},
	}))
	require.NoError(t, produce([]map[string]any{
		{"foo": "qux", "token": 3},
		{"foo": "zoom", "token": 4},
		{"foo": "thud", "token": 5},
		{"foo": "zing", "token": 6},
	}))
	require.NoError(t, produce([]map[string]any{
		{"foo": "bar", "token": 1},
		{"foo": "baz", "token": 2},
		{"foo": "qux", "token": 3},
		{"foo": "zoom", "token": 4},
	}))
	rows := RunSQLQuery(
		t,
		stream,
		`SELECT foo, token FROM integration_test_exactly_once ORDER BY token`,
	)
	require.Equal(t, [][]string{
		{"bar", "1"},
		{"baz", "2"},
		{"qux", "3"},
		{"zoom", "4"},
		{"thud", "5"},
		{"zing", "6"},
	}, rows)
}

func TestIntegrationNamedChannels(t *testing.T) {
	integration.CheckSkip(t)
	produce, stream := SetupSnowflakeStream(t, `
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_named_channels
  init_statement: |
    DROP TABLE IF EXISTS integration_test_named_channels;
  max_in_flight: 1
  offset_token: "${!this.token}"
  channel_name: "${!this.channel}"
  schema_evolution:
    enabled: true
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce([]map[string]any{
		{"foo": "bar", "token": 1, "channel": "foo"},
		{"foo": "baz", "token": 2, "channel": "foo"},
		{"foo": "qux", "token": 3, "channel": "foo"},
		{"foo": "zoom", "token": 4, "channel": "foo"},
	}))
	require.NoError(t, produce([]map[string]any{
		{"foo": "qux", "token": 3, "channel": "bar"},
		{"foo": "zoom", "token": 4, "channel": "bar"},
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
	}))
	require.NoError(t, produce([]map[string]any{
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
		{"foo": "bizz", "token": 7, "channel": "bar"},
		{"foo": "bang", "token": 8, "channel": "bar"},
	}))
	rows := RunSQLQuery(
		t,
		stream,
		`SELECT foo, token, channel FROM integration_test_named_channels ORDER BY channel, token`,
	)
	require.Equal(t, [][]string{
		{"qux", "3", "bar"},
		{"zoom", "4", "bar"},
		{"thud", "5", "bar"},
		{"zing", "6", "bar"},
		{"bizz", "7", "bar"},
		{"bang", "8", "bar"},
		{"bar", "1", "foo"},
		{"baz", "2", "foo"},
		{"qux", "3", "foo"},
		{"zoom", "4", "foo"},
	}, rows)
}

func TestIntegrationDynamicTables(t *testing.T) {
	integration.CheckSkip(t)
	produce, stream := SetupSnowflakeStream(t, `
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_dynamic_table_${!this.channel}
  init_statement: |
    DROP TABLE IF EXISTS integration_test_dynamic_table_foo;
    DROP TABLE IF EXISTS integration_test_dynamic_table_bar;
  max_in_flight: 4
  channel_name: "${!this.channel}"
  schema_evolution:
    enabled: true
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce([]map[string]any{
		{"foo": "bar", "token": 1, "channel": "foo"},
		{"foo": "baz", "token": 2, "channel": "foo"},
		{"foo": "qux", "token": 3, "channel": "foo"},
		{"foo": "zoom", "token": 4, "channel": "foo"},
	}))
	require.NoError(t, produce([]map[string]any{
		{"foo": "qux", "token": 3, "channel": "bar"},
		{"foo": "zoom", "token": 4, "channel": "bar"},
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
	}))
	require.NoError(t, produce([]map[string]any{
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
		{"foo": "bizz", "token": 7, "channel": "bar"},
		{"foo": "bang", "token": 8, "channel": "bar"},
	}))
	rows := RunSQLQuery(
		t,
		stream,
		`
    SELECT foo, token, channel, 'bar' AS "table" FROM integration_test_dynamic_table_bar
    UNION ALL
    SELECT foo, token, channel, 'foo' AS "table" FROM integration_test_dynamic_table_foo
    ORDER BY "table", channel, token;
    `,
	)
	require.Equal(t, [][]string{
		{"qux", "3", "bar", "bar"},
		{"zoom", "4", "bar", "bar"},
		{"thud", "5", "bar", "bar"},
		{"thud", "5", "bar", "bar"},
		{"zing", "6", "bar", "bar"},
		{"zing", "6", "bar", "bar"},
		{"bizz", "7", "bar", "bar"},
		{"bang", "8", "bar", "bar"},
		{"bar", "1", "foo", "foo"},
		{"baz", "2", "foo", "foo"},
		{"qux", "3", "foo", "foo"},
		{"zoom", "4", "foo", "foo"},
	}, rows)
}

func TestIntegrationSchemaEvolutionPipeline(t *testing.T) {
	integration.CheckSkip(t)
	produce, stream := SetupSnowflakeStream(t, `
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_auto_schema_evolution
  init_statement: |
    DROP TABLE IF EXISTS integration_test_auto_schema_evolution;
  max_in_flight: 4
  channel_name: "${!this.channel}"
  schema_evolution:
    enabled: true
    processors:
      - mapping: |
          root = match {
            this.name == "token" => "NUMBER"
            _ => "variant"
          }
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce([]map[string]any{
		{"foo": "bar", "token": 1, "channel": "foo"},
		{"foo": "baz", "token": 2, "channel": "foo"},
		{"foo": "qux", "token": 3, "channel": "foo"},
		{"foo": "zoom", "token": 4, "channel": "foo"},
	}))
	rows := RunSQLQuery(
		t,
		stream,
		`SELECT column_name, data_type, numeric_precision, numeric_scale FROM $DB.information_schema.columns WHERE table_name = 'INTEGRATION_TEST_AUTO_SCHEMA_EVOLUTION' AND table_schema = '$SCHEMA' ORDER BY column_name`,
	)
	require.Equal(t, [][]string{
		{"CHANNEL", "VARIANT", "", ""},
		{"FOO", "VARIANT", "", ""},
		{"TOKEN", "NUMBER", "38", "0"},
	}, rows)
}

func TestIntegrationSchemaEvolutionNull(t *testing.T) {
	integration.CheckSkip(t)
	runTest := func(t *testing.T, ignoreNull bool) {
		produce, stream := SetupSnowflakeStream(t, fmt.Sprintf(`
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_auto_schema_evolution_with_null
  init_statement: |
    DROP TABLE IF EXISTS integration_test_auto_schema_evolution_with_null;
  max_in_flight: 4
  channel_name: "${!this.channel}"
  schema_evolution:
    enabled: true
    ignore_nulls: %v
    processors:
      - mapping: |
          root = match {
            this.name == "null_a" || this.name == "null_b" => "NUMBER"
            _ => "variant"
          }
`, ignoreNull))
		RunStreamInBackground(t, stream)
		// Initial schema creation test
		require.NoError(t, produce([]map[string]any{
			{"foo": "bar", "null_a": nil},
		}))
		// Incremental schema migration test
		require.NoError(t, produce([]map[string]any{
			{"foo": "bar", "null_b": nil},
		}))
		rows := RunSQLQuery(
			t,
			stream,
			`SELECT column_name, data_type, numeric_precision, numeric_scale
     FROM $DB.information_schema.columns 
     WHERE table_name = 'INTEGRATION_TEST_AUTO_SCHEMA_EVOLUTION_WITH_NULL' AND table_schema = '$SCHEMA'
     ORDER BY column_name`,
		)
		if ignoreNull {
			require.Equal(t, [][]string{
				{"FOO", "VARIANT", "", ""},
			}, rows)
		} else {
			require.Equal(t, [][]string{
				{"FOO", "VARIANT", "", ""},
				{"NULL_A", "NUMBER", "38", "0"},
				{"NULL_B", "NUMBER", "38", "0"},
			}, rows)
		}
	}
	t.Run("IgnoreNull", func(t *testing.T) { runTest(t, true) })
	t.Run("IncludeNull", func(t *testing.T) { runTest(t, false) })
}

func TestIntegrationManualSchemaEvolution(t *testing.T) {
	// This is sort of a stress test for race conditions when the schema changes seperately
	integration.CheckSkip(t)
	produce, stream := SetupSnowflakeStream(t, `
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_manual_schema_evolution
  init_statement: |
    DROP TABLE IF EXISTS integration_test_manual_schema_evolution;
    CREATE TABLE integration_test_manual_schema_evolution(a VARIANT);
  max_in_flight: 10
  schema_evolution:
    enabled: true
    processors:
      - mapping: |
          root = this
          root.type = "variant"
      - sql_raw:
          driver: snowflake
          dsn: '$DSN'
          unsafe_dynamic_query: true
          query: |
            ALTER TABLE integration_test_manual_schema_evolution
              ADD COLUMN IF NOT EXISTS ${!this.name} ${!this.type}
      - mapping: |
          root = "variant"
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce([]map[string]any{
		{"a": 0},
	}))
	writers := []*asyncroutine.Periodic{}
	for range 10 {
		w := asyncroutine.NewPeriodic(10*time.Millisecond, func() {
			require.NoError(t, produce([]map[string]any{
				{"a": 0},
			}))
		})
		writers = append(writers, w)
		w.Start()
		t.Cleanup(w.Stop)
	}
	for c := range 10 {
		c := string([]byte{byte('b' + c)})
		t.Logf("Adding column: %q", c)
		require.NoError(t, produce([]map[string]any{
			{c: 0},
		}))
	}
	for _, w := range writers {
		w.Stop()
	}
}

func TestIntegrationTemporal(t *testing.T) {
	integration.CheckSkip(t)
	produce, stream := SetupSnowflakeStream(t, `
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_temporal
  init_statement: |
    DROP TABLE IF EXISTS integration_test_temporal;
    CREATE TABLE integration_test_temporal(a TIME, b TIMESTAMP_NTZ, c DATE);
  max_in_flight: 1
`)
	RunStreamInBackground(t, stream)
	d := 11*time.Hour + 35*time.Minute + 58*time.Second
	time := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Add(d)
	require.NoError(t, produce([]map[string]any{
		{"a": time, "b": time, "c": time},
	}))
	rows := RunSQLQuery(
		t,
		stream,
		`SELECT a, b, c FROM integration_test_temporal`,
	)
	require.Equal(t, [][]string{
		{"11:35:58", "0001-01-01 11:35:58.000", "0001-01-02"},
	}, rows)
}

func TestAllFloats(t *testing.T) {
	integration.CheckSkipExact(t)
	produce, stream := SetupSnowflakeStream(t, `
label: snowpipe_streaming
snowflake_streaming:
  account: "$ACCOUNT"
  user: "$USER"
  role: $ROLE
  database: "$DB"
  schema: $SCHEMA
  private_key_file: "$PRIVATE_KEY_FILE"
  table: integration_test_floats
  init_statement: |
    DROP TABLE IF EXISTS integration_test_floats;
    CREATE TABLE integration_test_floats(a FLOAT);
  max_in_flight: 16
`)
	RunStreamInBackground(t, stream)
	values := []float64{
		math.MaxFloat32, math.MaxFloat64, math.SmallestNonzeroFloat32, math.SmallestNonzeroFloat64,
		math.Pi, math.E, math.Sqrt2, math.Inf(1), math.Inf(-1), math.NaN(),
		0.0, math.Copysign(0, -1), 1e308, 1e-308, 1e-324,
		math.Ln2, math.Ln10, math.Log2E, math.Log10E, math.Phi,
	}
	var eg errgroup.Group
	eg.SetLimit(16)
	for set := range powerSet(values, 5) {
		batch := []map[string]any{}
		for _, f := range set {
			batch = append(batch, map[string]any{"a": f})
		}
		eg.Go(func() error { return produce(batch) })
	}
	require.NoError(t, eg.Wait())
	rows := RunSQLQuery(
		t,
		stream,
		`SELECT min(a), max(a) FROM integration_test_floats`,
	)
	require.Equal(t, [][]string{
		{"-inf", "NaN"},
	}, rows)
}

func powerSet[T any](items []T, minCount int) iter.Seq[[]T] {
	if len(items) >= 64 {
		return nil
	}
	return func(yield func([]T) bool) {
		for i := range uint64(1) << len(items) {
			// Make sure there are a few different numbers
			ones := bits.OnesCount64(i)
			if ones < minCount {
				continue
			}
			set := make([]T, 0, ones)
			for j := range items {
				mask := uint64(1) << j
				if i&mask != 0 {
					set = append(set, items[j])
				}
			}
			if !yield(set) {
				return
			}
		}
	}
}
