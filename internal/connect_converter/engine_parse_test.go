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
	// connector.class is retained in Props as well as promoted to Class.
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

func TestParseStripsLineComments(t *testing.T) {
	in := []byte(`{
// full-line comment
"connector.class": "io.example.Foo", // trailing comment
"name": "my-conn", // another trailing comment
"topics": "orders"
}`)
	cfg, err := parse(in)
	require.NoError(t, err)
	assert.Equal(t, "my-conn", cfg.Name)
	assert.Equal(t, "io.example.Foo", cfg.Class)
	assert.Equal(t, "orders", cfg.Props["topics"])
}

func TestParsePreservesSlashesInStrings(t *testing.T) {
	// Flat form with a // URL value and a full-line comment.
	in := []byte(`{
// primary database
"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
"connection.url": "jdbc:postgresql://h:5432/db",
"table.whitelist": "t"
}`)
	cfg, err := parse(in)
	require.NoError(t, err)
	assert.Equal(t, "jdbc:postgresql://h:5432/db", cfg.Props["connection.url"])
	assert.Equal(t, "t", cfg.Props["table.whitelist"])

	// Trailing comment on the same line as the URL value.
	in2 := []byte(`{
"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
"connection.url": "jdbc:mysql://h:3306/db", // primary
"table.whitelist": "t"
}`)
	cfg2, err := parse(in2)
	require.NoError(t, err)
	assert.Equal(t, "jdbc:mysql://h:3306/db", cfg2.Props["connection.url"])
}
