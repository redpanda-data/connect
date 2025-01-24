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
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func EnvOrDefault(name, fallback string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		value = fallback
	}
	return value
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

func SetupSnowflakeStream(t *testing.T, outputConfiguration string) (service.MessageBatchHandlerFunc, *service.Stream) {
	t.Helper()
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: DEBUG`))
	produce, err := streamBuilder.AddBatchProducerFunc()
	require.NoError(t, err)
	require.NoError(t, streamBuilder.AddOutputYAML(outputConfiguration))
	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	t.Cleanup(func() {
		err := stream.Stop(context.Background())
		require.NoError(t, err)
	})
	return produce, stream
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
	sql = strings.NewReplacer(
		"$DATABASE", EnvOrDefault("SNOWFLAKE_DB", "BABY_DATABASE"),
		"$SCHEMA", "PUBLIC",
	).Replace(sql)
	resp, err := client.RunSQL(context.Background(), streaming.RunSQLRequest{
		Statement: sql,
		Database:  EnvOrDefault("SNOWFLAKE_DB", "BABY_DATABASE"),
		Schema:    "PUBLIC",
		Role:      "ACCOUNTADMIN",
		Timeout:   30,
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
  account: "${SNOWFLAKE_ACCOUNT:WQKFXQQ-WI77362}"
  user: "${SNOWFLAKE_USER:ROCKWOODREDPANDA}"
  role: ACCOUNTADMIN
  database: "${SNOWFLAKE_DB:BABY_DATABASE}"
  schema: PUBLIC
  table: integration_test_exactly_once
  init_statement: |
    DROP TABLE IF EXISTS integration_test_exactly_once;
  private_key_file: "${SNOWFLAKE_PRIVATE_KEY:./streaming/resources/rsa_key.p8}"
  max_in_flight: 1
  offset_token: "${!this.token}"
  schema_evolution:
    enabled: true
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "bar", "token": 1},
		{"foo": "baz", "token": 2},
		{"foo": "qux", "token": 3},
		{"foo": "zoom", "token": 4},
	})))
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "qux", "token": 3},
		{"foo": "zoom", "token": 4},
		{"foo": "thud", "token": 5},
		{"foo": "zing", "token": 6},
	})))
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "bar", "token": 1},
		{"foo": "baz", "token": 2},
		{"foo": "qux", "token": 3},
		{"foo": "zoom", "token": 4},
	})))
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
  account: "${SNOWFLAKE_ACCOUNT:WQKFXQQ-WI77362}"
  user: "${SNOWFLAKE_USER:ROCKWOODREDPANDA}"
  role: ACCOUNTADMIN
  database: "${SNOWFLAKE_DB:BABY_DATABASE}"
  schema: PUBLIC
  table: integration_test_named_channels
  init_statement: |
    DROP TABLE IF EXISTS integration_test_named_channels;
  private_key_file: "${SNOWFLAKE_PRIVATE_KEY:./streaming/resources/rsa_key.p8}"
  max_in_flight: 1
  offset_token: "${!this.token}"
  channel_name: "${!this.channel}"
  schema_evolution:
    enabled: true
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "bar", "token": 1, "channel": "foo"},
		{"foo": "baz", "token": 2, "channel": "foo"},
		{"foo": "qux", "token": 3, "channel": "foo"},
		{"foo": "zoom", "token": 4, "channel": "foo"},
	})))
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "qux", "token": 3, "channel": "bar"},
		{"foo": "zoom", "token": 4, "channel": "bar"},
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
	})))
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
		{"foo": "bizz", "token": 7, "channel": "bar"},
		{"foo": "bang", "token": 8, "channel": "bar"},
	})))
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
  account: "${SNOWFLAKE_ACCOUNT:WQKFXQQ-WI77362}"
  user: "${SNOWFLAKE_USER:ROCKWOODREDPANDA}"
  role: ACCOUNTADMIN
  database: "${SNOWFLAKE_DB:BABY_DATABASE}"
  schema: PUBLIC
  table: integration_test_dynamic_table_${!this.channel}
  init_statement: |
    DROP TABLE IF EXISTS integration_test_dynamic_table_foo;
    DROP TABLE IF EXISTS integration_test_dynamic_table_bar;
  private_key_file: "${SNOWFLAKE_PRIVATE_KEY:./streaming/resources/rsa_key.p8}"
  max_in_flight: 4
  channel_name: "${!this.channel}"
  schema_evolution:
    enabled: true
`)
	RunStreamInBackground(t, stream)
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "bar", "token": 1, "channel": "foo"},
		{"foo": "baz", "token": 2, "channel": "foo"},
		{"foo": "qux", "token": 3, "channel": "foo"},
		{"foo": "zoom", "token": 4, "channel": "foo"},
	})))
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "qux", "token": 3, "channel": "bar"},
		{"foo": "zoom", "token": 4, "channel": "bar"},
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
	})))
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "thud", "token": 5, "channel": "bar"},
		{"foo": "zing", "token": 6, "channel": "bar"},
		{"foo": "bizz", "token": 7, "channel": "bar"},
		{"foo": "bang", "token": 8, "channel": "bar"},
	})))
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
  account: "${SNOWFLAKE_ACCOUNT:WQKFXQQ-WI77362}"
  user: "${SNOWFLAKE_USER:ROCKWOODREDPANDA}"
  role: ACCOUNTADMIN
  database: "${SNOWFLAKE_DB:BABY_DATABASE}"
  schema: PUBLIC
  table: integration_test_pipeline
  init_statement: |
    DROP TABLE IF EXISTS integration_test_pipeline;
  private_key_file: "${SNOWFLAKE_PRIVATE_KEY:./streaming/resources/rsa_key.p8}"
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
	require.NoError(t, produce(context.Background(), Batch([]map[string]any{
		{"foo": "bar", "token": 1, "channel": "foo"},
		{"foo": "baz", "token": 2, "channel": "foo"},
		{"foo": "qux", "token": 3, "channel": "foo"},
		{"foo": "zoom", "token": 4, "channel": "foo"},
	})))
	rows := RunSQLQuery(
		t,
		stream,
		`SELECT column_name, data_type, numeric_precision, numeric_scale FROM $DATABASE.information_schema.columns WHERE table_name = 'INTEGRATION_TEST_PIPELINE' AND table_schema = '$SCHEMA' ORDER BY column_name`,
	)
	require.Equal(t, [][]string{
		{"CHANNEL", "VARIANT", "", ""},
		{"FOO", "VARIANT", "", ""},
		{"TOKEN", "NUMBER", "38", "0"},
	}, rows)
}
