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

func TestLookupConnectorMiss_ReturnsFallback(t *testing.T) {
	m := lookupConnector("does.not.Exist")
	ctx := newTestCtx(map[string]any{"connector.class": "does.not.Exist"})
	comp, err := m.Map(ctx.cfg, ctx)
	require.NoError(t, err)
	require.NotNil(t, comp.Output)

	out, err := yaml.Marshal(comp.Output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "TODO")
	assert.NotEmpty(t, ctx.Warnings())
}

func TestRegisterAndLookupConnector(t *testing.T) {
	orig := connectorMappers
	connectorMappers = map[string]ConnectorMapper{}
	t.Cleanup(func() { connectorMappers = orig })

	registerConnector("test.Only", stubConnector{})
	m := lookupConnector("test.Only")
	_, ok := m.(stubConnector)
	assert.True(t, ok)
}

type stubConnector struct{}

func (stubConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	return Component{Output: component("drop", mapping())}, nil
}
