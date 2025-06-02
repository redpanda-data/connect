// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/gateway"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestJWTConfigErrors(t *testing.T) {
	for _, test := range []struct {
		name        string
		values      map[string]string
		errContains string
	}{
		{
			name:   "nothing set",
			values: map[string]string{},
		},
		{
			name: "everything set",
			values: map[string]string{
				"REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL":      "http://localhost:1234",
				"REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE":        "foo",
				"REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID": "bar",
			},
		},
		{
			name: "no audience no org",
			values: map[string]string{
				"REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL": "http://localhost:1234",
			},
			errContains: "requires an audience",
		},
		{
			name: "no org",
			values: map[string]string{
				"REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL": "http://localhost:1234",
				"REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE":   "foo",
			},
			errContains: "requires an org",
		},
		{
			name: "invalid issuer url",
			values: map[string]string{
				"REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL":      "::://nahnope",
				"REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE":        "foo",
				"REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID": "bar",
			},
			errContains: "missing protocol scheme",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for k, v := range test.values {
				t.Setenv(k, v)
			}

			mgr := service.MockResources()
			license.InjectTestService(mgr)

			_, err := gateway.NewRPJWTMiddleware(mgr)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestJWTLicenseCheckNotApplicable(t *testing.T) {
	mgr := service.MockResources()

	_, err := gateway.NewRPJWTMiddleware(mgr)
	require.NoError(t, err)
}

func TestJWTLicenseCheckValid(t *testing.T) {
	for k, v := range map[string]string{
		"REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL":      "http://localhost:1234",
		"REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE":        "foo",
		"REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID": "bar",
	} {
		t.Setenv(k, v)
	}

	mgr := service.MockResources()
	license.InjectTestService(mgr)

	_, err := gateway.NewRPJWTMiddleware(mgr)
	require.NoError(t, err)
}

func TestJWTLicenseCheckInvalid(t *testing.T) {
	for k, v := range map[string]string{
		"REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL":      "http://localhost:1234",
		"REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE":        "foo",
		"REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID": "bar",
	} {
		t.Setenv(k, v)
	}

	mgr := service.MockResources()

	_, err := gateway.NewRPJWTMiddleware(mgr)
	require.Error(t, err)
}
