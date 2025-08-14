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

package cohere

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

type TestMessageCollector struct {
	mu    sync.Mutex
	batch service.MessageBatch
}

func (c *TestMessageCollector) Collect(_ context.Context, msg *service.Message) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batch = append(c.batch, msg)
	return nil
}

func (c *TestMessageCollector) GetMessages() service.MessageBatch {
	return slices.Clone(c.batch)
}

func TestToolCallingIntegration(t *testing.T) {
	integration.CheckSkip(t)
	if os.Getenv("COHERE_API_KEY") == "" {
		t.Skip("Skipping test because COHERE_API_KEY is not set")
	}
	builder := service.NewStreamBuilder()
	handler, err := builder.AddProducerFunc()
	require.NoError(t, err)
	var collector TestMessageCollector
	require.NoError(t, builder.AddConsumerFunc(collector.Collect))
	err = builder.AddProcessorYAML(`
cohere_chat:
  api_key: "${COHERE_API_KEY}"
  model: command-r-plus
  prompt: "What is the weather near me? You will probably need to lookup my location first"
  tools:
    - name: "get_user_location"
      description: "Get the user's location"
      parameters: {}
      processors:
        - mapping: 'root.location = "New York City"'
    - name: "get_weather"
      description: "Get the weather for a location"
      parameters:
        required: ["city"]
        properties:
          city:
            type: string
            description: "The city to get the weather for"
      processors:
        - mapping: |
            if !this.city.contains("New York") {
              throw("Wrong city")
            }
        - mapping: 'root.weather = "Slightly sunny and 68 degrees"'
    `)
	require.NoError(t, err)
	stream, err := builder.Build()
	license.InjectTestService(stream.Resources())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := stream.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		require.NoError(t, err)
	}()
	err = handler(t.Context(), service.NewMessage([]byte(`"hello"`)))
	require.NoError(t, err)
	cancel()
	<-done
	batch := collector.GetMessages()
	require.Len(t, batch, 1)
	require.NoError(t, batch[0].GetError())
	msg, err := batch[0].AsBytes()
	require.NoError(t, err)
	require.Contains(t, string(msg), `68`)
	t.Log("got:", string(msg))
}
