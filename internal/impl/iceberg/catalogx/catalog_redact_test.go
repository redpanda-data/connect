// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package catalogx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCatalogClientURLRedacted(t *testing.T) {
	_, err := NewCatalogClient(t.Context(), Config{
		URL:      "http://u:S3cretPass@bad host:8181",
		AuthType: "none",
	}, []string{"ns"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
