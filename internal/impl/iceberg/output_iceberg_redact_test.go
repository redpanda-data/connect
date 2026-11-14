// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCatalogConfigOAuth2ServerURIRedacted(t *testing.T) {
	conf, err := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
  auth:
    oauth2:
      client_id: id
      client_secret: secret
      server_uri: "http://u:S3cretPass@bad host:8181/token"
namespace: ns
table: t
storage:
  aws_s3:
    bucket: bucket
`, nil)
	require.NoError(t, err)

	_, err = parseCatalogConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing oauth2 server URI")
	assert.NotContains(t, err.Error(), "S3cretPass")
}
