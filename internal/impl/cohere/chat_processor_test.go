/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package cohere

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/stretchr/testify/require"
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
	ctx, cancel := context.WithCancel(context.Background())
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
	err = handler(context.Background(), service.NewMessage([]byte(`"hello"`)))
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
