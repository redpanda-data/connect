// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tigerbeetle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestConfigLinting(t *testing.T) {
	linter := service.NewEnvironment().NewComponentConfigLinter()

	tests := []struct {
		name    string
		conf    string
		lintErr string
	}{
		{
			name: "basic config",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
`,
		},
		{
			name: "advanced config",
			conf: `
tigerbeetle_cdc:
  cluster_id: 181161957064799711348825326453165787824
  addresses: [ "127.0.0.1:3000", "127.0.0.1:3001", "127.0.0.1:3002" ]
  progress_cache: foocache
  event_count_max: 1024
  idle_interval_ms: 5000
  timestamp_initial: 1756549800322811551
`,
		},
		{
			name: "invalid cluster_id",
			conf: `
tigerbeetle_cdc:
  cluster_id: xyz
  addresses: [ "3000" ]
  progress_cache: foocache
`,
			lintErr: "(3,1) field 'cluster_id' must be a valid integer",
		},
		{
			name: "empty cluster_id",
			conf: `
tigerbeetle_cdc:
  cluster_id:
  addresses: [ "3000" ]
  progress_cache: foocache
`,
			lintErr: "(3,1) field 'cluster_id' must be a valid integer",
		},
		{
			name: "missing cluster_id",
			conf: `
tigerbeetle_cdc:
  addresses: [ "3000" ]
  progress_cache: foocache
`,
			lintErr: "(3,1) field cluster_id is required",
		},
		{
			name: "empty addresses",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ ]
  progress_cache: foocache
`,
			lintErr: "(4,1) field 'addresses' must contain at least one address",
		},
		{
			name: "missing progress_cache",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
`,
			lintErr: "(3,1) field progress_cache is required",
		},
		{
			name: "zeroed event_count_max",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
  event_count_max: 0
`,
			lintErr: "(6,1) field 'event_count_max' must be greater than 0",
		},
		{
			name: "negative event_count_max",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
  event_count_max: -1
`,
			lintErr: "(6,1) field 'event_count_max' must be greater than 0",
		},
		{
			name: "zeroed idle_interval_ms",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
  idle_interval_ms: 0
`,
			lintErr: "(6,1) field 'idle_interval_ms' must be greater than 0",
		},
		{
			name: "negative idle_interval_ms",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
  idle_interval_ms: -1
`,
			lintErr: "(6,1) field 'idle_interval_ms' must be greater than 0",
		},
		{
			name: "negative timestamp_initial",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
  timestamp_initial: -1
`,
			lintErr: "(6,1) field 'timestamp_initial' must be a valid integer",
		},
		{
			name: "invalid timestamp_initial",
			conf: `
tigerbeetle_cdc:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
  timestamp_initial: xyz
`,
			lintErr: "(6,1) field 'timestamp_initial' must be a valid integer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lints, err := linter.LintInputYAML([]byte(test.conf))
			require.NoError(t, err)
			if test.lintErr != "" {
				assert.Len(t, lints, 1)
				assert.Equal(t, test.lintErr, lints[0].Error())
			} else {
				assert.Empty(t, lints)
			}
		})
	}
}
