// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestLicenseEnterpriseNoLicense(t *testing.T) {
	tmpDir := t.TempDir()
	tmpBadLicensePath := filepath.Join(tmpDir, "bad.license")

	res := service.MockResources()
	RegisterService(res, Config{
		customDefaultLicenseFilepath: tmpBadLicensePath,
	})

	loaded, err := LoadFromResources(res)
	require.NoError(t, err)

	assert.False(t, loaded.AllowsEnterpriseFeatures())
}
