// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenPgConnectionBadDSNRedacted(t *testing.T) {
	const secret = "S3cretPass"
	_, err := openPgConnectionFromConfig(&Config{
		DBRawDSN: "postgres://user@localhost:5432/db?password=" + secret + "&sslmode=bogus",
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "sslmode")
	assert.NotContains(t, err.Error(), secret)
}
