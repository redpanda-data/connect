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

func TestLicenseDevLicenseDefault(t *testing.T) {
	tmpDir := t.TempDir()
	res := service.MockResources()
	RegisterService(res, Config{
		customDefaultLicenseFilepath: filepath.Join(tmpDir, "missing.license"),
	})

	loaded, err := LoadFromResources(res)
	require.NoError(t, err)

	// No production license → embedded dev license with enterprise access and throttler.
	assert.True(t, loaded.AllowsEnterpriseFeatures())

	svc := getSharedService(res)
	require.NotNil(t, svc)
	assert.True(t, svc.isTestLicense)
	assert.NotNil(t, getThrottler(res))
}
