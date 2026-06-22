// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertS3Sink(t *testing.T) {
	in := []byte(`{
	  "name":"s3-sink",
	  "config":{
	    "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	    "s3.bucket.name":"my-bucket",
	    "s3.region":"us-east-1",
	    "topics":"orders"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "aws_s3:")
	assert.Contains(t, y, "bucket: my-bucket")
	assert.Contains(t, y, "region: us-east-1")
	assert.Contains(t, y, "path:")
	// input side is a TODO stub for a sink.
	assert.Contains(t, y, "TODO: set the input")
}
