// Copyright 2026 Redpanda Data, Inc.
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
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// The consumer lag gauge name must not depend on whether unordered processing
// is enabled, otherwise flipping that field silently breaks lag dashboards and
// alerts.
func TestFranzReaderToggledLagMetricName(t *testing.T) {
	tests := []struct {
		name string
		conf string
	}{
		{
			name: "ordered",
			conf: `
consumer_group: foogroup
`,
		},
		{
			name: "unordered",
			conf: `
consumer_group: foogroup
unordered_processing:
  enabled: true
`,
		},
		{
			name: "unordered explicitly disabled",
			conf: `
consumer_group: foogroup
unordered_processing:
  enabled: false
`,
		},
	}

	spec := service.NewConfigSpec().Fields(FranzReaderToggledConfigFields()...)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.conf, nil)
			require.NoError(t, err)

			rdr, err := NewFranzReaderToggledFromConfig(conf, service.MockResources(), func() ([]kgo.Opt, error) {
				return nil, nil
			})
			require.NoError(t, err)

			var lagMetricName string
			switch r := rdr.(type) {
			case *FranzReaderOrdered:
				lagMetricName = r.lagMetricName
			case *FranzReaderUnordered:
				lagMetricName = r.lagMetricName
			default:
				t.Fatalf("unexpected reader type %T", rdr)
			}

			assert.Equal(t, lagMetricNameRedpanda, lagMetricName)
		})
	}
}
