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

package kubernetes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKubernetesEventsConfigParse(t *testing.T) {
	spec := kubernetesEventsInputConfig()

	tests := []struct {
		name        string
		config      string
		expectError bool
	}{
		{
			name:        "minimal config",
			config:      ``,
			expectError: false,
		},
		{
			name: "filter by warning events only",
			config: `
types:
  - Warning
`,
			expectError: false,
		},
		{
			name: "filter by involved kinds",
			config: `
involved_kinds:
  - Pod
  - Node
types:
  - Warning
`,
			expectError: false,
		},
		{
			name: "full config",
			config: `
namespaces:
  - default
  - kube-system
label_selector: ""
field_selector: ""
types:
  - Normal
  - Warning
involved_kinds:
  - Pod
  - Deployment
reasons:
  - Failed
  - FailedScheduling
include_existing: true
max_event_age: "2h"
auto_auth: true
`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(tt.config, nil)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, conf)
			}
		})
	}
}
