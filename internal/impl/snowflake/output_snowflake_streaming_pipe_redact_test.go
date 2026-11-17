// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnowpipeStreamingPipeURLErrorRedactsCredentials(t *testing.T) {
	yaml := fmt.Sprintf(`
account: org-account
url: "http://u:S3cretPass@localhost:bad"
user: USER
role: ROLE
database: DB
schema: SCHEMA
pipe: PIPE
private_key: %q
`, generatedPrivateKeyBase64(t))
	conf, err := snowpipeStreamingPipeOutputConfig().ParseYAML(yaml, nil)
	require.NoError(t, err)

	_, _, _, err = newSnowpipeStreamingPipeOutput(conf, testResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid url")
	assert.NotContains(t, err.Error(), "S3cretPass")
}
