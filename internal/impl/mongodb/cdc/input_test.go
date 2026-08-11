// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSpecParsesAWSBlock(t *testing.T) {
	sb := service.NewStreamBuilder()
	err := sb.AddInputYAML(`
mongodb_cdc:
  url: "mongodb://localhost:27017"
  database: foo
  collections: [bar]
  checkpoint_cache: foocache
  aws:
    enabled: true
    region: us-east-1
    roles:
      - role: arn:aws:iam::123456789012:role/foo
`)
	require.NoError(t, err)
}
