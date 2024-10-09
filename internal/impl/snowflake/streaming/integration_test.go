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
	"os"
	"testing"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	"github.com/stretchr/testify/require"
)

func TestSnowflake(t *testing.T) {
	ctx := context.Background()
	privateKeyFile, err := os.ReadFile("./resources/rsa_key.p8")
	require.NoError(t, err)
	block, _ := pem.Decode(privateKeyFile)
	require.NoError(t, err)
	parseResult, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	require.NoError(t, err)
	client, err := streaming.NewSnowflakeServiceClient(ctx, streaming.ClientOptions{
		Account:    "UOKKRAZ-OJ08711",
		User:       "ROCKWOODREDPANDAAWS",
		Role:       "ACCOUNTADMIN",
		PrivateKey: parseResult.(*rsa.PrivateKey),
	})
	require.NoError(t, err)
	defer client.Close()
	channel, err := client.OpenChannel(ctx, streaming.ChannelOptions{
		Name:         "my_first_testing_channel",
		DatabaseName: "AWS_DB",
		SchemaName:   "PUBLIC",
		TableName:    "AWS_TABLE",
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		err = channel.InsertRows(ctx, []map[string]any{
			{"A": "foo"},
			{"A": "bar"},
			{"A": "baz"},
			//{"A": 0, "B": "qyz", "C": true},
			//{"A": 0, "B": "solid", "C": true},
			//{"A": -1, "B": "wow!", "C": true},
			//{"C1": "foo", "C2": "bar", "C3": "baz"},
			//{"C1": "a", "C2": "b", "C3": "c"},
			//{"C1": "1", "C2": "2", "C3": "3"},
		})
		require.NoError(t, err)
	}
}
