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
	"encoding/pem"
	"errors"
	"fmt"
	"os"
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

func TestSnowflake(t *testing.T) {
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
		Account:    "WQKFXQQ-WI77362",
		User:       "ROCKWOODREDPANDA",
		Role:       "ACCOUNTADMIN",
		PrivateKey: parseResult.(*rsa.PrivateKey),
	}
	channelOpts := streaming.ChannelOptions{
		Name:         "my_first_testing_channel",
		DatabaseName: "BABY_DATABASE",
		SchemaName:   "PUBLIC",
		TableName:    "TEST_TABLE_KITCHEN_SINK",
	}
	restClient, err := streaming.NewRestClient(clientOptions.Account, clientOptions.User, clientOptions.PrivateKey, clientOptions.Logger)
	require.NoError(t, err)
	defer restClient.Close()
	_, err = restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Database: channelOpts.DatabaseName,
		Schema:   channelOpts.SchemaName,
		Statement: fmt.Sprintf(`
      DROP TABLE IF EXISTS %s;
      CREATE TABLE %s (
        A STRING,
        B BOOLEAN
      );`, channelOpts.TableName, channelOpts.TableName),
		Parameters: map[string]string{
			"MULTI_STATEMENT_COUNT": "0",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err = restClient.RunSQL(ctx, streaming.RunSQLRequest{
			Database:  channelOpts.DatabaseName,
			Schema:    channelOpts.SchemaName,
			Statement: fmt.Sprintf(`DROP TABLE %s;`, channelOpts.TableName),
		})
		if err != nil {
			t.Log("unable to cleanup table in SNOW:", err)
		}
	})
	streamClient, err := streaming.NewSnowflakeServiceClient(ctx, clientOptions)
	require.NoError(t, err)
	defer streamClient.Close()
	channel, err := streamClient.OpenChannel(ctx, channelOpts)
	require.NoError(t, err)
	_, err = channel.InsertRows(ctx, service.MessageBatch{
		msg(`{
      "A": "bar",
      "B": true
    }`),
		msg(`{
      "A": "baz",
      "B": "false"
    }`),
		msg(`{
      "A": "foo",
      "B": null
    }`),
	})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// Always order by A so we get consistent ordering for our test
		resp, err := restClient.RunSQL(ctx, streaming.RunSQLRequest{
			Database:  channelOpts.DatabaseName,
			Schema:    channelOpts.SchemaName,
			Statement: fmt.Sprintf(`SELECT * FROM %s ORDER BY A;`, channelOpts.TableName),
		})
		if !assert.NoError(collect, err) {
			return
		}
		assert.Equal(collect, "00000", resp.SQLState)
		assert.Equal(collect, [][]string{
			{"bar", "true"},
			{"baz", "false"},
			{"foo", ""},
		}, resp.Data)
	}, 5*time.Second, time.Second)
}
