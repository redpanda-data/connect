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

func TestCheckRunningEnterpriseWithoutLicense(t *testing.T) {
	tests := []struct {
		name            string
		registerService bool
	}{
		{
			name:            "license service registered without a license",
			registerService: true,
		},
		{
			name:            "no license service registered",
			registerService: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := service.MockResources()
			if test.registerService {
				RegisterService(res, Config{
					customDefaultLicenseFilepath: filepath.Join(t.TempDir(), "missing.license"),
				})
			}

			err := CheckRunningEnterprise(res)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "requires a valid Redpanda Enterprise Edition license")
		})
	}
}
