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

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestKafkaOutputIdempotentWriteConfig(t *testing.T) {
	tests := []struct {
		name                string
		yaml                string
		wantIdempotent      bool
		wantRequiredAcks    sarama.RequiredAcks
		wantMaxOpenRequests int
	}{
		{
			name: "idempotent write without ack_replicas",
			yaml: `
addresses: [ localhost:9092 ]
topic: foo
idempotent_write: true
`,
			wantIdempotent:      true,
			wantRequiredAcks:    sarama.WaitForAll,
			wantMaxOpenRequests: 1,
		},
		{
			name: "idempotent write with ack_replicas",
			yaml: `
addresses: [ localhost:9092 ]
topic: foo
idempotent_write: true
ack_replicas: true
`,
			wantIdempotent:      true,
			wantRequiredAcks:    sarama.WaitForAll,
			wantMaxOpenRequests: 1,
		},
		{
			name: "no idempotent write keeps sarama defaults",
			yaml: `
addresses: [ localhost:9092 ]
topic: foo
`,
			wantIdempotent:      false,
			wantRequiredAcks:    sarama.WaitForLocal,
			wantMaxOpenRequests: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := OSKConfigSpec().ParseYAML(tc.yaml, nil)
			require.NoError(t, err)

			k := kafkaWriter{mgr: service.MockResources()}
			saramaConf, err := k.saramaConfigFromParsed(conf)
			require.NoError(t, err)

			require.NoError(t, saramaConf.Validate())
			assert.Equal(t, tc.wantIdempotent, saramaConf.Producer.Idempotent)
			assert.Equal(t, tc.wantRequiredAcks, saramaConf.Producer.RequiredAcks)
			assert.Equal(t, tc.wantMaxOpenRequests, saramaConf.Net.MaxOpenRequests)
		})
	}
}
