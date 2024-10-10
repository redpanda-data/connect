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
	"os"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
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
	client, err := streaming.NewSnowflakeServiceClient(ctx, streaming.ClientOptions{
		Account:    "WQKFXQQ-WI77362",
		User:       "ROCKWOODREDPANDA",
		Role:       "ACCOUNTADMIN",
		PrivateKey: parseResult.(*rsa.PrivateKey),
	})
	require.NoError(t, err)
	defer client.Close()
	channel, err := client.OpenChannel(ctx, streaming.ChannelOptions{
		Name:         "my_first_testing_channel",
		DatabaseName: "BABY_DATABASE",
		SchemaName:   "PUBLIC",
		TableName:    "TEST_TABLE",
	})
	require.NoError(t, err)
	for i := 0; i < 1; i++ {
		err = channel.InsertRows(ctx, service.MessageBatch{
			msg(`{"A": 42}`),
		})
		require.NoError(t, err)
	}
}
