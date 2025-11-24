// Copyright 2025 Redpanda Data, Inc.
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

package redpanda

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestTracerConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		expectErr string
	}{
		{
			name: "valid_basic_config",
			config: `
seed_brokers: ["localhost:9092"]
topic: "test-traces"
`,
			expectErr: "",
		},
		{
			name: "cloud_sa_without_initialization",
			config: `
seed_brokers: ["localhost:9092"]
topic: "test-traces"
use_redpanda_cloud_service_account: true
`,
			expectErr: "failed to get Redpanda Cloud service account token source",
		},
		{
			name: "cloud_sa_with_explicit_sasl",
			config: `
seed_brokers: ["localhost:9092"]
topic: "test-traces"
use_redpanda_cloud_service_account: true
sasl:
  - mechanism: PLAIN
    username: test
    password: test
`,
			expectErr: "use_redpanda_cloud_service_account cannot be used together with explicit sasl configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := tracerSpec()
			parsed, err := spec.ParseYAML(tt.config, nil)
			require.NoError(t, err)

			_, err = tracerConfigFromParsed(parsed, service.MockResources().Logger())

			if tt.expectErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr)
			}
		})
	}
}

func TestTracerConfigValidation_Errors(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		expectErr string
	}{
		{
			name: "invalid_format",
			config: `
seed_brokers: ["localhost:9092"]
topic: "test-traces"
format: "invalid"
`,
			expectErr: "unknown `format` value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := tracerSpec()
			parsed, err := spec.ParseYAML(tt.config, nil)
			if err != nil {
				// Parsing itself may fail for missing required fields
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErr)
				return
			}

			_, err = tracerConfigFromParsed(parsed, service.MockResources().Logger())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectErr)
		})
	}
}
