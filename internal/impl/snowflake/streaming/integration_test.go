// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package streaming_test

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
)

func ptr[T any](v T) *T {
	return &v
}

func msg(s string) *service.Message {
	return service.NewMessage([]byte(s))
}

func structuredMsg(v any) *service.Message {
	msg := service.NewMessage(nil)
	msg.SetStructured(v)
	return msg
}

func envOr(name, dflt string) string {
	val := os.Getenv(name)
	if val != "" {
		return val
	}
	return dflt
}

func setup(t *testing.T) (*streaming.SnowflakeRestClient, *streaming.SnowflakeServiceClient) {
	t.Helper()
	ctx := t.Context()
	privateKeyFile, err := os.ReadFile("./resources/rsa_key.p8")
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("no RSA private key, skipping snowflake test")
	}
	require.NoError(t, err)
	block, _ := pem.Decode(privateKeyFile)
	require.NoError(t, err)
	parseResult, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	require.NoError(t, err)
	clientOptions := streaming.ClientOptions{
		Account:        envOr("SNOWFLAKE_ACCOUNT", "WQKFXQQ-WI77362"),
		URL:            fmt.Sprintf("https://%s.snowflakecomputing.com", envOr("SNOWFLAKE_ACCOUNT", "WQKFXQQ-WI77362")),
		User:           envOr("SNOWFLAKE_USER", "ROCKWOODREDPANDA"),
		Role:           "ACCOUNTADMIN",
		PrivateKey:     parseResult.(*rsa.PrivateKey),
		ConnectVersion: "",
	}
	restClient, err := streaming.NewRestClient(streaming.RestOptions{
		Account:    clientOptions.Account,
		User:       clientOptions.User,
		URL:        clientOptions.URL,
		Version:    clientOptions.ConnectVersion,
		PrivateKey: clientOptions.PrivateKey,
		Logger:     clientOptions.Logger,
	})
	require.NoError(t, err)
	t.Cleanup(restClient.Close)
	streamClient, err := streaming.NewSnowflakeServiceClient(ctx, clientOptions)
	require.NoError(t, err)
	t.Cleanup(streamClient.Close)
	return restClient, streamClient
}

func TestAllSnowflakeDatatypes(t *testing.T) {
	ctx := t.Context()
	restClient, streamClient := setup(t)
	channelOpts := streaming.ChannelOptions{
		Name:         t.Name(),
		DatabaseName: envOr("SNOWFLAKE_DB", "BABY_DATABASE"),
		SchemaName:   "PUBLIC",
		TableName:    "TEST_TABLE_KITCHEN_SINK",
		BuildOptions: streaming.BuildOptions{Parallelism: 1, ChunkSize: 50_000},
	}
	_, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Database: channelOpts.DatabaseName,
		Schema:   channelOpts.SchemaName,
		Statement: fmt.Sprintf(`
      DROP TABLE IF EXISTS %s;
      CREATE TABLE %s (
        A STRING,
        B BOOLEAN,
        C VARIANT,
        D ARRAY,
        E OBJECT,
        F REAL,
        G NUMBER,
        H TIME,
        I DATE,
        J TIMESTAMP_LTZ,
        K TIMESTAMP_NTZ,
        L TIMESTAMP_TZ
      );`, channelOpts.TableName, channelOpts.TableName),
		Parameters: map[string]string{
			"MULTI_STATEMENT_COUNT": "0",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = streamClient.DropChannel(ctx, channelOpts)
		if err != nil {
			t.Log("unable to cleanup stream in SNOW:", err)
		}
	})
	channel, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	_, err = channel.InsertRows(ctx, service.MessageBatch{
		msg(`{
      "A": "bar",
      "B": true,
      "C": {"foo": "bar"},
      "D": [[42], null, {"A":"B"}],
      "E": {"foo":"bar"},
      "F": 3.14,
      "G": -1,
      "H": "2024-01-01T13:02:06Z",
      "I": "2007-11-03T00:00:00Z",
      "J": "2024-01-01T12:00:00.000Z",
      "K": "2024-01-01T12:00:00.000-08:00",
      "L": "2024-01-01T12:00:00.000-08:00"
    }`),
		msg(`{
      "A": "baz",
      "B": "false",
      "C": {"a":"b"},
      "D": [1, 2, 3],
      "E": {"foo":"baz"},
      "F": 42.12345,
      "G": 9,
      "H": "2024-01-02T13:02:06.123456789Z",
      "I": "2019-03-04T00:00:00.12345Z",
      "J": "1970-01-02T12:00:00.000Z",
      "K": "2024-02-01T12:00:00.000-08:00",
      "L": "2024-01-01T12:00:01.000-08:00"
    }`),
		msg(`{
      "A": "foo",
      "B": null,
      "C": [1, 2, 3],
      "D": ["a", 9, "z"],
      "E": {"baz":"qux"},
      "F": -0.0,
      "G": 42,
      "H": 1728680106,
      "I": 1728680106,
      "J": "2024-01-03T12:00:00.000-08:00",
      "K": "2024-01-01T13:00:00.000-08:00",
      "L": "2024-01-01T12:30:00.000-08:00"
    }`),
	}, nil)
	require.NoError(t, err)
	time.Sleep(time.Second)
	// Always order by A so we get consistent ordering for our test
	resp, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Database:  channelOpts.DatabaseName,
		Schema:    channelOpts.SchemaName,
		Statement: fmt.Sprintf(`SELECT * FROM %s ORDER BY A;`, channelOpts.TableName),
		Parameters: map[string]string{
			"TIMESTAMP_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
			"DATE_OUTPUT_FORMAT":      "YYYY-MM-DD",
			"TIME_OUTPUT_FORMAT":      "HH24:MI:SS",
		},
	})
	assert.Equal(t, "00000", resp.SQLState)
	expected := [][]string{
		{
			`bar`,
			`true`,
			`{"foo":"bar"}`,
			`[[42], null, {"A":"B"}]`,
			`{"foo": "bar"}`,
			`3.14`,
			`-1`,
			`13:02:06`,
			`2007-11-03`,
			`2024-01-01 04:00:00.000 -0800`,
			`2024-01-01 20:00:00.000`,
			`2024-01-01 12:00:00.000 -0800`,
		},
		{
			`baz`,
			`false`,
			`{"a":"b"}`,
			`[1, 2, 3]`,
			`{"foo":"baz"}`,
			`42.12345`,
			`9`,
			`13:02:06`,
			`2019-03-04`,
			`1970-01-02 04:00:00.000 -0800`,
			`2024-02-01 20:00:00.000`,
			`2024-01-01 12:00:01.000 -0800`,
		},
		{
			`foo`,
			``,
			`[1, 2, 3]`,
			`["a", 9, "z"]`,
			`{"baz":"qux"}`,
			`-0.0`,
			`42`,
			`20:55:06`,
			`2024-10-11`,
			`2024-01-03 12:00:00.000 -0800`,
			`2024-01-01 21:00:00.000`,
			`2024-01-01 12:30:00.000 -0800`,
		},
	}
	assert.Equal(t, parseSnowflakeData(expected), parseSnowflakeData(resp.Data))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// Make sure stats are written correctly by doing a query that only needs to read from epInfo
		resp, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
			Database: channelOpts.DatabaseName,
			Schema:   channelOpts.SchemaName,
			Statement: fmt.Sprintf(`SELECT
          MAX(A), MAX(B), MAX(C),
                          MAX(F),
          MAX(G), MAX(H), MAX(I),
          MAX(J), MAX(K), MAX(L)
          FROM %s`, channelOpts.TableName),
			Parameters: map[string]string{
				"TIMESTAMP_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
				"DATE_OUTPUT_FORMAT":      "YYYY-MM-DD",
				"TIME_OUTPUT_FORMAT":      "HH24:MI:SS",
			},
		})
		if !assert.NoError(collect, err) {
			t.Logf("failed to scan table: %s", err)
			return
		}
		assert.Equal(collect, "00000", resp.SQLState)
		expected := [][]string{
			{
				`foo`,
				`true`,
				`[1, 2, 3]`,
				`42.12345`,
				`42`,
				`20:55:06`,
				`2024-10-11`,
				`2024-01-03 12:00:00.000 -0800`,
				`2024-02-01 20:00:00.000`,
				`2024-01-01 12:30:00.000 -0800`,
			},
		}
		assert.Equal(collect, parseSnowflakeData(expected), parseSnowflakeData(resp.Data))
	}, 3*time.Second, time.Second)
}

func TestIntegerCompat(t *testing.T) {
	ctx := t.Context()
	restClient, streamClient := setup(t)
	channelOpts := streaming.ChannelOptions{
		Name:         t.Name(),
		DatabaseName: envOr("SNOWFLAKE_DB", "BABY_DATABASE"),
		SchemaName:   "PUBLIC",
		TableName:    "TEST_INT_TABLE",
		BuildOptions: streaming.BuildOptions{Parallelism: 1, ChunkSize: 50_000},
	}
	_, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Database: channelOpts.DatabaseName,
		Schema:   channelOpts.SchemaName,
		Statement: fmt.Sprintf(`
      DROP TABLE IF EXISTS %s;
      CREATE TABLE IF NOT EXISTS %s (
        A NUMBER,
        B NUMBER(38, 8),
        C NUMBER(18, 0),
        D NUMBER(28, 8)
      );`, channelOpts.TableName, channelOpts.TableName),
		Parameters: map[string]string{
			"MULTI_STATEMENT_COUNT": "0",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = streamClient.DropChannel(ctx, channelOpts)
		if err != nil {
			t.Log("unable to cleanup stream in SNOW:", err)
		}
	})
	channel, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	_, err = channel.InsertRows(ctx, service.MessageBatch{
		structuredMsg(map[string]any{
			"a": math.MinInt64,
			"b": math.MinInt8,
			"c": math.MaxInt32,
			"d": math.MinInt8,
		}),
		structuredMsg(map[string]any{
			"a": 0,
			"b": "0.12345678",
			"c": 0,
		}),
		structuredMsg(map[string]any{
			"a": math.MaxInt64,
			"b": math.MaxInt8,
			"c": math.MaxInt16,
			"d": "1234.12345678",
		}),
	}, nil)
	require.NoError(t, err)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// Always order by A so we get consistent ordering for our test
		resp, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
			Database:  channelOpts.DatabaseName,
			Schema:    channelOpts.SchemaName,
			Statement: fmt.Sprintf(`SELECT * FROM %s ORDER BY A;`, channelOpts.TableName),
		})
		if !assert.NoError(collect, err) {
			t.Logf("failed to scan table: %s", err)
			return
		}
		assert.Equal(collect, "00000", resp.SQLState)
		itoa := strconv.Itoa
		assert.Equal(collect, parseSnowflakeData([][]string{
			{itoa(math.MinInt64), itoa(math.MinInt8), itoa(math.MaxInt32), itoa(math.MinInt8)},
			{"0", "0.12345678", "0", ""},
			{itoa(math.MaxInt64), itoa(math.MaxInt8), itoa(math.MaxInt16), "1234.12345678"},
		}), parseSnowflakeData(resp.Data))
	}, 3*time.Second, time.Second)
}

func TestTimestampCompat(t *testing.T) {
	ctx := t.Context()
	restClient, streamClient := setup(t)
	channelOpts := streaming.ChannelOptions{
		Name:         t.Name(),
		DatabaseName: envOr("SNOWFLAKE_DB", "BABY_DATABASE"),
		SchemaName:   "PUBLIC",
		TableName:    "TEST_TIMESTAMP_TABLE",
		BuildOptions: streaming.BuildOptions{Parallelism: 1, ChunkSize: 50_000},
	}
	var columnDefs []string
	var columnNames []string
	for _, tsType := range []string{"_NTZ", "_TZ", "_LTZ"} {
		for precision := range make([]int, 10) {
			name := fmt.Sprintf("TS%s_%d", tsType, precision)
			columnNames = append(columnNames, name)
			columnDefs = append(columnDefs, name+fmt.Sprintf(" TIMESTAMP%s(%d)", tsType, precision))
		}
	}
	_, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Database: channelOpts.DatabaseName,
		Schema:   channelOpts.SchemaName,
		Statement: fmt.Sprintf(`
      DROP TABLE IF EXISTS %s;
      CREATE TABLE IF NOT EXISTS %s (
        %s
      );`, channelOpts.TableName, channelOpts.TableName, strings.Join(columnDefs, ", ")),
		Parameters: map[string]string{
			"MULTI_STATEMENT_COUNT": "0",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = streamClient.DropChannel(ctx, channelOpts)
		if err != nil {
			t.Log("unable to cleanup stream in SNOW:", err)
		}
	})
	channel, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	timestamps1 := map[string]any{}
	timestamps2 := map[string]any{}
	easternTz, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	for _, col := range columnNames {
		timestamps1[col] = time.Date(
			2024, 1, 0o1,
			12, 30, 0o5,
			int(time.Nanosecond+time.Microsecond+time.Millisecond),
			time.UTC,
		)
		timestamps2[col] = time.Date(
			2024, 1, 0o1,
			20, 45, 55,
			int(time.Nanosecond+time.Microsecond+time.Millisecond),
			easternTz,
		)
	}
	_, err = channel.InsertRows(ctx, service.MessageBatch{
		structuredMsg(timestamps1),
		structuredMsg(timestamps2),
		msg(`{}`), // all nulls
	}, nil)
	require.NoError(t, err)
	expectedRows := [][]string{
		{
			"2024-01-01 12:30:05.000",
			"2024-01-01 12:30:05.000",
			"2024-01-01 12:30:05.000",
			"2024-01-01 12:30:05.001",
			"2024-01-01 12:30:05.001",
			"2024-01-01 12:30:05.001",
			"2024-01-01 12:30:05.001",
			"2024-01-01 12:30:05.001",
			"2024-01-01 12:30:05.001",
			"2024-01-01 12:30:05.001",
			"2024-01-01 12:30:05. Z",
			"2024-01-01 12:30:05.0 Z",
			"2024-01-01 12:30:05.00 Z",
			"2024-01-01 12:30:05.001 Z",
			"2024-01-01 12:30:05.0010 Z",
			"2024-01-01 12:30:05.00100 Z",
			"2024-01-01 12:30:05.001001 Z",
			"2024-01-01 12:30:05.0010010 Z",
			"2024-01-01 12:30:05.00100100 Z",
			"2024-01-01 12:30:05.001001001 Z",
			"2024-01-01 04:30:05. -0800",
			"2024-01-01 04:30:05.0 -0800",
			"2024-01-01 04:30:05.00 -0800",
			"2024-01-01 04:30:05.001 -0800",
			"2024-01-01 04:30:05.0010 -0800",
			"2024-01-01 04:30:05.00100 -0800",
			"2024-01-01 04:30:05.001001 -0800",
			"2024-01-01 04:30:05.0010010 -0800",
			"2024-01-01 04:30:05.00100100 -0800",
			"2024-01-01 04:30:05.001001001 -0800",
		},
		{
			"2024-01-02 01:45:55.000",
			"2024-01-02 01:45:55.000",
			"2024-01-02 01:45:55.000",
			"2024-01-02 01:45:55.001",
			"2024-01-02 01:45:55.001",
			"2024-01-02 01:45:55.001",
			"2024-01-02 01:45:55.001",
			"2024-01-02 01:45:55.001",
			"2024-01-02 01:45:55.001",
			"2024-01-02 01:45:55.001",
			"2024-01-01 20:45:55. -0500",
			"2024-01-01 20:45:55.0 -0500",
			"2024-01-01 20:45:55.00 -0500",
			"2024-01-01 20:45:55.001 -0500",
			"2024-01-01 20:45:55.0010 -0500",
			"2024-01-01 20:45:55.00100 -0500",
			"2024-01-01 20:45:55.001001 -0500",
			"2024-01-01 20:45:55.0010010 -0500",
			"2024-01-01 20:45:55.00100100 -0500",
			"2024-01-01 20:45:55.001001001 -0500",
			"2024-01-01 17:45:55. -0800",
			"2024-01-01 17:45:55.0 -0800",
			"2024-01-01 17:45:55.00 -0800",
			"2024-01-01 17:45:55.001 -0800",
			"2024-01-01 17:45:55.0010 -0800",
			"2024-01-01 17:45:55.00100 -0800",
			"2024-01-01 17:45:55.001001 -0800",
			"2024-01-01 17:45:55.0010010 -0800",
			"2024-01-01 17:45:55.00100100 -0800",
			"2024-01-01 17:45:55.001001001 -0800",
		},
		make([]string, 30),
	}
	require.EventuallyWithT(t, func(*assert.CollectT) {
		resp, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
			Database:  channelOpts.DatabaseName,
			Schema:    channelOpts.SchemaName,
			Statement: fmt.Sprintf(`SELECT * FROM %s ORDER BY TS_NTZ_9;`, channelOpts.TableName),
			Parameters: map[string]string{
				"TIMESTAMP_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF TZHTZM",
			},
		})
		if !assert.NoError(t, err) {
			t.Logf("failed to scan table: %s", err)
			return
		}
		assert.Equal(t, "00000", resp.SQLState)
		assert.Equal(t, parseSnowflakeData(expectedRows), parseSnowflakeData(resp.Data))
	}, 3*time.Second, time.Second)
}

func TestChannelReopenFails(t *testing.T) {
	ctx := t.Context()
	restClient, streamClient := setup(t)
	channelOpts := streaming.ChannelOptions{
		Name:         t.Name(),
		DatabaseName: envOr("SNOWFLAKE_DB", "BABY_DATABASE"),
		SchemaName:   "PUBLIC",
		TableName:    "TEST_CHANNEL_TABLE",
		BuildOptions: streaming.BuildOptions{Parallelism: 1, ChunkSize: 50_000},
	}
	_, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Database: channelOpts.DatabaseName,
		Schema:   channelOpts.SchemaName,
		Statement: fmt.Sprintf(`
      DROP TABLE IF EXISTS %s;
      CREATE TABLE IF NOT EXISTS %s (
        A NUMBER
      );`, channelOpts.TableName, channelOpts.TableName),
		Parameters: map[string]string{
			"MULTI_STATEMENT_COUNT": "0",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = streamClient.DropChannel(ctx, channelOpts)
		if err != nil {
			t.Log("unable to cleanup stream in SNOW:", err)
		}
	})
	channelA, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	channelB, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	_, err = channelA.InsertRows(ctx, service.MessageBatch{
		structuredMsg(map[string]any{"a": math.MinInt64}),
		structuredMsg(map[string]any{"a": 0}),
		structuredMsg(map[string]any{"a": math.MaxInt64}),
	}, nil)
	require.Error(t, err)
	_, err = channelB.InsertRows(ctx, service.MessageBatch{
		structuredMsg(map[string]any{"a": math.MinInt64}),
		structuredMsg(map[string]any{"a": 0}),
		structuredMsg(map[string]any{"a": math.MaxInt64}),
	}, nil)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// Always order by A so we get consistent ordering for our test
		resp, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
			Database:  channelOpts.DatabaseName,
			Schema:    channelOpts.SchemaName,
			Statement: fmt.Sprintf(`SELECT * FROM %s ORDER BY A;`, channelOpts.TableName),
		})
		if !assert.NoError(collect, err) {
			t.Logf("failed to scan table: %s", err)
			return
		}
		assert.Equal(collect, "00000", resp.SQLState)
		itoa := strconv.Itoa
		assert.Equal(collect, parseSnowflakeData([][]string{
			{itoa(math.MinInt64)},
			{"0"},
			{itoa(math.MaxInt64)},
		}), parseSnowflakeData(resp.Data))
	}, 3*time.Second, time.Second)
}

func TestChannelOffsetToken(t *testing.T) {
	ctx := t.Context()
	restClient, streamClient := setup(t)
	channelOpts := streaming.ChannelOptions{
		Name:         t.Name(),
		DatabaseName: envOr("SNOWFLAKE_DB", "BABY_DATABASE"),
		SchemaName:   "PUBLIC",
		TableName:    "TEST_OFFSET_TOKEN_TABLE",
		BuildOptions: streaming.BuildOptions{Parallelism: 1, ChunkSize: 50_000},
	}
	_, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Database: channelOpts.DatabaseName,
		Schema:   channelOpts.SchemaName,
		Statement: fmt.Sprintf(`
      DROP TABLE IF EXISTS %s;
      CREATE TABLE IF NOT EXISTS %s (
        A NUMBER
      );`, channelOpts.TableName, channelOpts.TableName),
		Parameters: map[string]string{
			"MULTI_STATEMENT_COUNT": "0",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = streamClient.DropChannel(ctx, channelOpts)
		if err != nil {
			t.Log("unable to cleanup stream in SNOW:", err)
		}
	})
	channelA, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	require.Nil(t, channelA.LatestOffsetToken())
	_, err = channelA.InsertRows(ctx, service.MessageBatch{
		structuredMsg(map[string]any{"a": math.MinInt64}),
		structuredMsg(map[string]any{"a": 0}),
		structuredMsg(map[string]any{"a": math.MaxInt64}),
	}, &streaming.OffsetTokenRange{Start: "3", End: "5"})
	require.NoError(t, err)
	require.EqualValues(t, ptr(streaming.OffsetToken("5")), channelA.LatestOffsetToken())
	_, err = channelA.InsertRows(ctx, service.MessageBatch{
		structuredMsg(map[string]any{"a": -1}),
		structuredMsg(map[string]any{"a": 0}),
		structuredMsg(map[string]any{"a": 1}),
	}, &streaming.OffsetTokenRange{Start: "0", End: "2"})
	require.NoError(t, err)
	require.Equal(t, ptr(streaming.OffsetToken("2")), channelA.LatestOffsetToken())
	_, err = channelA.WaitUntilCommitted(ctx, time.Minute)
	require.NoError(t, err)
	channelB, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	require.Equal(t, ptr(streaming.OffsetToken("2")), channelB.LatestOffsetToken())
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// Always order by A so we get consistent ordering for our test
		resp, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
			Database:  channelOpts.DatabaseName,
			Schema:    channelOpts.SchemaName,
			Statement: fmt.Sprintf(`SELECT * FROM %s ORDER BY A;`, channelOpts.TableName),
		})
		if !assert.NoError(collect, err) {
			t.Logf("failed to scan table: %s", err)
			return
		}
		assert.Equal(collect, "00000", resp.SQLState)
		itoa := strconv.Itoa
		assert.Equal(collect, parseSnowflakeData([][]string{
			{itoa(math.MinInt64)},
			{"-1"},
			{"0"},
			{"0"},
			{"1"},
			{itoa(math.MaxInt64)},
		}), parseSnowflakeData(resp.Data))
	}, 3*time.Second, time.Second)
}

// parseSnowflakeData returns "json-ish" data that can be JSON or could be just a raw string.
// We want to parse for the JSON rows have whitespace, so this gives us a more semantic comparison.
func parseSnowflakeData(rawData [][]string) [][]any {
	var parsedData [][]any
	for _, rawRow := range rawData {
		var parsedRow []any
		for _, rawCol := range rawRow {
			var parsedCol any
			if rawCol != `` {
				err := json.Unmarshal([]byte(rawCol), &parsedCol)
				if err != nil {
					parsedCol = rawCol
				}
			}
			parsedRow = append(parsedRow, parsedCol)
		}
		parsedData = append(parsedData, parsedRow)
	}
	return parsedData
}
