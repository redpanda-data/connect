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

func TestParseRESTWrapped(t *testing.T) {
	in := []byte(`{"name":"my-conn","config":{"connector.class":"io.example.Foo","topics":"orders"}}`)
	cfg, err := parse(in)
	require.NoError(t, err)
	assert.Equal(t, "my-conn", cfg.Name)
	assert.Equal(t, "io.example.Foo", cfg.Class)
	assert.Equal(t, "orders", cfg.Props["topics"])
	// connector.class must NOT remain a stray prop key beyond Class.
	assert.Equal(t, "io.example.Foo", cfg.Props["connector.class"])
}

func TestParseFlat(t *testing.T) {
	in := []byte(`{"connector.class":"io.example.Foo","name":"my-conn","topics":"orders"}`)
	cfg, err := parse(in)
	require.NoError(t, err)
	assert.Equal(t, "my-conn", cfg.Name)
	assert.Equal(t, "io.example.Foo", cfg.Class)
	assert.Equal(t, "orders", cfg.Props["topics"])
}

func TestParseMalformed(t *testing.T) {
	_, err := parse([]byte(`{not json`))
	require.Error(t, err)
}

func TestParseMissingClass(t *testing.T) {
	_, err := parse([]byte(`{"name":"x","config":{"topics":"orders"}}`))
	require.Error(t, err)
}
