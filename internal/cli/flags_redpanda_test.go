// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedpandaConnDetailsParserSimple(t *testing.T) {
	details, err := rpConnDetails(
		[]string{"foo", "bar"},
		false,
		"",
		false,
		"", "", "",
	)
	require.NoError(t, err)

	assert.Len(t, details.SeedBrokers, 2)
	assert.Equal(t, "foo", details.SeedBrokers[0])
	assert.Equal(t, "bar", details.SeedBrokers[1])

	assert.False(t, details.TLSEnabled)

	assert.Equal(t, time.Second*20, details.ConnIdleTimeout)
	assert.Equal(t, time.Minute*5, details.MetaMaxAge)
}

func TestRedpandaConnDetailsParserTLS(t *testing.T) {
	details, err := rpConnDetails(
		[]string{"foo", "bar"},
		true,
		"",
		false,
		"", "", "",
	)
	require.NoError(t, err)

	assert.Len(t, details.SeedBrokers, 2)
	assert.Equal(t, "foo", details.SeedBrokers[0])
	assert.Equal(t, "bar", details.SeedBrokers[1])

	assert.True(t, details.TLSEnabled)
}
