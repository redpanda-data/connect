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
	"gopkg.in/yaml.v3"
)

func TestComponentRendersSingleKeyMap(t *testing.T) {
	body := mapping()
	kv(body, "bucket", scalar("my-bucket"))
	comp := component("aws_s3", body)

	out, err := yaml.Marshal(comp)
	require.NoError(t, err)
	assert.Equal(t, "aws_s3:\n    bucket: my-bucket\n", string(out))
}

func TestSeqBuildsSequence(t *testing.T) {
	s := seq(scalar("a"), scalar("b"))
	out, err := yaml.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, "- a\n- b\n", string(out))
}
