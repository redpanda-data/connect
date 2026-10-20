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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/common-go/license"
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

func TestCheckRunningEnterprise(t *testing.T) {
	validExpiry := time.Now().Add(time.Hour).Unix()
	pastExpiry := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		name string
		// license is the license the license service holds. Nil means that no
		// license service is registered, as in redpanda-connect-community.
		license license.RedpandaLicense
		// wantErr is a substring of the expected error. Empty means no error.
		wantErr string
	}{
		{
			name:    "no license service",
			license: nil,
			wantErr: "this feature requires a valid Redpanda Enterprise Edition license that includes the Connect product",
		},
		{
			name:    "open source license",
			license: openSourceLicense,
			wantErr: "this feature requires a valid Redpanda Enterprise Edition license that includes the Connect product",
		},
		{
			name: "v1 enterprise license with Connect",
			license: &license.V1RedpandaLicense{
				Type:     license.LicenseTypeEnterprise,
				Expiry:   validExpiry,
				Products: []license.Product{license.ProductConnect},
			},
			wantErr: "",
		},
		{
			name: "v1 free trial license with Connect",
			license: &license.V1RedpandaLicense{
				Type:     license.LicenseTypeFreeTrial,
				Expiry:   validExpiry,
				Products: []license.Product{license.ProductConnect},
			},
			wantErr: "",
		},
		{
			name: "v1 enterprise license without Connect",
			license: &license.V1RedpandaLicense{
				Type:     license.LicenseTypeEnterprise,
				Expiry:   validExpiry,
				Products: []license.Product{"OTHER"},
			},
			wantErr: "this feature requires a valid Redpanda Enterprise Edition license that includes the Connect product",
		},
		{
			name: "v1 expired enterprise license with Connect",
			license: &license.V1RedpandaLicense{
				Type:     license.LicenseTypeEnterprise,
				Expiry:   pastExpiry,
				Products: []license.Product{license.ProductConnect},
			},
			wantErr: "this feature requires a valid Redpanda Enterprise Edition license that includes the Connect product",
		},
		{
			name: "v0 enterprise license",
			license: &license.V0RedpandaLicense{
				Type:   license.V0LicenseTypeEnterprise,
				Expiry: validExpiry,
			},
			wantErr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := service.MockResources()
			if test.license != nil {
				s := &Service{
					logger:        res.Logger(),
					loadedLicense: &atomic.Pointer[license.RedpandaLicense]{},
				}
				s.setLicense(res, test.license)
				// setLicense starts an expiry metric loop only for licenses that
				// allow enterprise features.
				if s.cancel != nil {
					t.Cleanup(s.cancel)
				}
				setSharedService(res, s)
			}

			err := CheckRunningEnterprise(res)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}
