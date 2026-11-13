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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestKafkaBadParams(t *testing.T) {
	testCases := []struct {
		name   string
		topics []string
		errStr string
	}{
		{
			name:   "mixing consumer types",
			topics: []string{"foo", "foo:1"},
			errStr: "it is not currently possible to include balanced and explicit partition topics in the same kafka input",
		},
		{
			name:   "too many partitions",
			topics: []string{"foo:1:2:3"},
			errStr: "topic 'foo:1:2:3' is invalid, only one partition and an optional offset should be specified",
		},
		{
			name:   "bad range",
			topics: []string{"foo:1-2-3"},
			errStr: "partition '1-2-3' is invalid, only one range can be specified",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := iskConfigSpec().ParseYAML(fmt.Sprintf(`
addresses: [ example.com:1234 ]
topics: %v
`, gabs.Wrap(test.topics).String()), nil)
			require.NoError(t, err)

			_, err = newKafkaReaderFromParsed(pConf, service.MockResources())
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errStr)
		})
	}
}

func TestKafkaExplicitPartitionsCloseStopsConsumers(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys([]sarama.ApiVersionsResponseKey{
			{ApiKey: 1, MinVersion: 0, MaxVersion: 11}, // Fetch
			{ApiKey: 2, MinVersion: 0, MaxVersion: 5},  // ListOffsets
			{ApiKey: 3, MinVersion: 0, MaxVersion: 7},  // Metadata
		}),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("foo", 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("foo", 0, sarama.OffsetOldest, 0).
			SetOffset("foo", 0, sarama.OffsetNewest, 0),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1),
	})

	pConf, err := iskConfigSpec().ParseYAML(fmt.Sprintf(`
addresses: [ %v ]
topics: [ foo:0 ]
`, broker.Addr()), nil)
	require.NoError(t, err)

	k, err := newKafkaReaderFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	require.NoError(t, k.Connect(ctx))
	require.NoError(t, k.Close(ctx))

	// Close returns only after the commit goroutine stops, and that goroutine
	// sets msgChan to nil as its last step.
	k.cMut.Lock()
	defer k.cMut.Unlock()
	assert.Nil(t, k.msgChan)
}
