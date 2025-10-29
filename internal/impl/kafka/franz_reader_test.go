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

package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestFranzConsumerDetailsFromConfig(t *testing.T) {
	tests := []struct {
		name          string
		config        string
		wantTopics    []string
		wantRegexMode bool
		wantExclude   []string
	}{
		{
			name: "topics_only",
			config: `
topics:
  - foo
  - bar
`,
			wantTopics:    []string{"foo", "bar"},
			wantRegexMode: false,
		},
		{
			name: "regexp_topics_include",
			config: `
regexp_topics_include:
  - "logs_.*"
  - "metrics_.*"
`,
			wantTopics:    []string{"logs_.*", "metrics_.*"},
			wantRegexMode: true,
		},
		{
			name: "regexp_include_with_exclude",
			config: `
regexp_topics_include:
  - "logs_.*"
regexp_topics_exclude:
  - "logs_debug_.*"
`,
			wantTopics:    []string{"logs_.*"},
			wantRegexMode: true,
			wantExclude:   []string{"logs_debug_.*"},
		},
		{
			name: "deprecated_regexp_topics_true",
			config: `
topics:
  - "logs_.*"
regexp_topics: true
`,
			wantTopics:    []string{"logs_.*"},
			wantRegexMode: true,
		},
		{
			name: "deprecated_regexp_topics_with_exclude",
			config: `
topics:
  - "logs_.*"
regexp_topics: true
regexp_topics_exclude:
  - "logs_debug_.*"
`,
			wantTopics:    []string{"logs_.*"},
			wantRegexMode: true,
			wantExclude:   []string{"logs_debug_.*"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := service.NewEnvironment()

			spec := service.NewConfigSpec().
				Fields(FranzConsumerFields()...)

			pConf, err := spec.ParseYAML(tc.config, env)
			require.NoError(t, err)

			got, err := FranzConsumerDetailsFromConfig(pConf)
			require.NoError(t, err)

			assert.Equal(t, tc.wantTopics, got.Topics)
			assert.Equal(t, tc.wantRegexMode, got.RegexPattern)
			if tc.wantExclude != nil {
				assert.Equal(t, tc.wantExclude, got.ExcludeTopics)
			}
		})
	}
}
