// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package secrets

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLookupURNsBadURNRedacted(t *testing.T) {
	_, err := ParseLookupURNs(t.Context(), slog.Default(), "redis://u:S3cretPass@bad host:6379")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
