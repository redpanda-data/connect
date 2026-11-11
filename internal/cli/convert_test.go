// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertCliBuilds(t *testing.T) {
	cmd := convertCli()
	require.NotNil(t, cmd)
	assert.Equal(t, "convert", cmd.Name)
}

func TestConvertCliHasServerSubcommand(t *testing.T) {
	cmd := convertCli()
	var found bool
	for _, sub := range cmd.Subcommands {
		if sub.Name == "server" {
			found = true
		}
	}
	assert.True(t, found, "convert should have a 'server' subcommand")
}
