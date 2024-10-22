// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package streaming_test

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func msg(s string) *service.Message {
	return service.NewMessage([]byte(s))
}

func setup(t *testing.T) (*streaming.SnowflakeRestClient, *streaming.SnowflakeServiceClient) {
	t.Helper()
	ctx := context.Background()
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
		Account:        "WQKFXQQ-WI77362",
		User:           "ROCKWOODREDPANDA",
		Role:           "ACCOUNTADMIN",
		PrivateKey:     parseResult.(*rsa.PrivateKey),
		ConnectVersion: "v4.0.0-dev",
		Application:    "development",
	}
	restClient, err := streaming.NewRestClient(
		clientOptions.Account,
		clientOptions.User,
		"v4.0.0-dev",
		"Redpanda_Connect_development",
		clientOptions.PrivateKey,
		clientOptions.Logger,
	)
	require.NoError(t, err)
	t.Cleanup(restClient.Close)
	streamClient, err := streaming.NewSnowflakeServiceClient(ctx, clientOptions)
	require.NoError(t, err)
	t.Cleanup(func() { _ = streamClient.Close() })
	return restClient, streamClient
}

func TestSnowflake(t *testing.T) {
	ctx := context.Background()
	restClient, streamClient := setup(t)
	channelOpts := streaming.ChannelOptions{
		Name:         t.Name(),
		DatabaseName: "BABY_DATABASE",
		SchemaName:   "PUBLIC",
		TableName:    "TEST_TABLE_KITCHEN_SINK",
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
      "K": "2024-01-01T12:00:00.000-08:00",
      "L": "2024-01-01T12:00:00.000-08:00"
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
      "K": "2024-01-01T12:00:00.000-08:00",
      "L": "2024-01-01T12:00:00.000-08:00"
    }`),
	})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
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
		if !assert.NoError(collect, err) {
			t.Logf("failed to scan table: %s", err)
			return
		}
		assert.Equal(collect, "00000", resp.SQLState)
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
				`-1832551390-01-03 00:00:00.000 -0800`,
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
				`-1832551390-01-05 00:00:00.000 -0800`,
				`2024-01-01 20:00:00.000`,
				`2024-01-01 12:00:00.000 -0800`,
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
				`-1832551390-01-08 00:00:00.000 -0800`,
				`2024-01-01 20:00:00.000`,
				`2024-01-01 12:00:00.000 -0800`,
			},
		}
		assert.Equal(collect, parseSnowflakeData(expected), parseSnowflakeData(resp.Data))
	}, 3*time.Second, time.Second)
}

func TestIntegerCompat(t *testing.T) {
	ctx := context.Background()
	restClient, streamClient := setup(t)
	channelOpts := streaming.ChannelOptions{
		Name:         t.Name(),
		DatabaseName: "BABY_DATABASE",
		SchemaName:   "PUBLIC",
		TableName:    "TEST_INT_TABLE",
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
	channel, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	expectedInts := []int{}
	rows := [][2]int{
		{-1, 1},
		{math.MinInt8, math.MaxInt8},
		{math.MinInt16, math.MaxInt16},
		{math.MinInt32, math.MaxInt32},
		{math.MinInt64, math.MaxInt64},
	}
	for _, row := range rows {
		_, err = channel.InsertRows(ctx, service.MessageBatch{
			msg(`{
        "a": ` + strconv.Itoa(row[0]) + ` 
      }`),
			msg(`{
        "a": 0
      }`),
			msg(`{
        "a": ` + strconv.Itoa(row[1]) + ` 
      }`),
		})
		require.NoError(t, err)
		expectedInts = append(
			expectedInts,
			row[0],
			0,
			row[1],
		)
	}
	slices.Sort(expectedInts)
	expectedRows := [][]string{}
	for _, i := range expectedInts {
		expectedRows = append(expectedRows, []string{strconv.Itoa(i)})
	}
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
		assert.Equal(collect, parseSnowflakeData(expectedRows), parseSnowflakeData(resp.Data))
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
