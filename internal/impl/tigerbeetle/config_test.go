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
tigerbeetle:
  cluster_id: 0
  addresses: [ "3000" ]
  progress_cache: foocache
`,
		},
		{
			name: "advanced config",
			conf: `
tigerbeetle:
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
tigerbeetle:
  cluster_id: xyz
  addresses: [ "3000" ]
  progress_cache: foocache
`,
			lintErr: "(3,1) field 'cluster_id' must be a valid integer",
		},
		{
			name: "empty cluster_id",
			conf: `
tigerbeetle:
  cluster_id:
  addresses: [ "3000" ]
  progress_cache: foocache
`,
			lintErr: "(3,1) field 'cluster_id' must be a valid integer",
		},
		{
			name: "missing cluster_id",
			conf: `
tigerbeetle:
  addresses: [ "3000" ]
  progress_cache: foocache
`,
			lintErr: "(3,1) field cluster_id is required",
		},
		{
			name: "empty addresses",
			conf: `
tigerbeetle:
  cluster_id: 0
  addresses: [ ]
  progress_cache: foocache
`,
			lintErr: "(4,1) field 'addresses' must contain at least one address",
		},
		{
			name: "missing progress_cache",
			conf: `
tigerbeetle:
  cluster_id: 0
  addresses: [ "3000" ]
`,
			lintErr: "(3,1) field progress_cache is required",
		},
		{
			name: "zeroed event_count_max",
			conf: `
tigerbeetle:
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
tigerbeetle:
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
tigerbeetle:
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
tigerbeetle:
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
tigerbeetle:
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
tigerbeetle:
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
