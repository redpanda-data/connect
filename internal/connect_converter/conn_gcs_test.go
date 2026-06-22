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

func TestConvertGCSSink(t *testing.T) {
	in := []byte(`{"name":"gcs","config":{"connector.class":"io.confluent.connect.gcs.GcsSinkConnector","gcs.bucket.name":"bkt","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "gcp_cloud_storage:")
	assert.Contains(t, y, "bucket: bkt")
	assert.Contains(t, y, "path:")
}
