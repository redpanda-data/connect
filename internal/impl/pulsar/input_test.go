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

package pulsar

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestParseInputTopicXorPattern(t *testing.T) {
	tests := []struct {
		name, config string
		errStr       string
	}{
		{
			name:   "topics",
			config: `topics: ["my_cool_topic"]`,
		},
		{
			name:   "topics_pattern",
			config: `topics_pattern: ".*cool_topic"`,
		},
		{
			name:   "topics and topics_pattern fails",
			errStr: "exactly one of fields topics and topics_pattern must be set",
			config: `
topics: ["my_cool_topic"]
topics_pattern: ".*_cool_topic"
`,
		},
		{
			name:   "providing neither fails",
			errStr: "exactly one of fields topics and topics_pattern must be set",
			config: ``,
		},
	}

	baseConfig := `
url: pulsar://localhost:6650/
subscription_name: "sub"
`
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := service.NewEnvironment()

			conf := baseConfig + test.config
			parsed, err := inputConfigSpec().ParseYAML(conf, env)
			require.NoError(t, err, "parse config")

			reader, err := newPulsarReaderFromParsed(parsed, service.MockResources().Logger())
			if test.errStr != "" {
				require.EqualError(t, err, test.errStr)
			} else {
				require.NoError(t, err, "new reader from parsed")
				require.NoError(t, reader.Close(t.Context()))
			}
		})
	}
}
