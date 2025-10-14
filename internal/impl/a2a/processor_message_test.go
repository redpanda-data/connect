// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package a2a

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestA2AMessageIntegration(t *testing.T) {
	integration.CheckSkip(t)

	// Check required OAuth2 env vars
	if os.Getenv(RPEnvTokenURL) == "" {
		t.Skipf("Skipping test because %s is not set", RPEnvTokenURL)
	}
	if os.Getenv(RPEnvClientID) == "" {
		t.Skipf("Skipping test because %s is not set", RPEnvClientID)
	}
	if os.Getenv(RPEnvClientSecret) == "" {
		t.Skipf("Skipping test because %s is not set", RPEnvClientSecret)
	}

	// Use a test agent card URL - update this to point to your test agent
	agentCardURL := "https://your-test-agent.example.com"

	builder := service.NewStreamBuilder()
	handler, err := builder.AddProducerFunc()
	require.NoError(t, err)

	var receivedMsg *service.Message
	require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		receivedMsg = msg
		return nil
	}))

	err = builder.AddProcessorYAML(`
a2a_message:
  agent_card_url: "` + agentCardURL + `"
  prompt: "Say 'Hello from integration test' and nothing else"
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

	// Send test message
	require.NoError(t, handler(t.Context(), service.NewMessage([]byte("test input"))))

	cancel()
	<-done

	// Verify response
	require.NotNil(t, receivedMsg, "No response received from A2A agent")
	responseBytes, err := receivedMsg.AsBytes()
	require.NoError(t, err)
	require.NotEmpty(t, responseBytes)

	// Check metadata
	taskID, exists := receivedMsg.MetaGetMut("a2a_task_id")
	require.True(t, exists)
	require.NotEmpty(t, taskID)

	contextID, exists := receivedMsg.MetaGetMut("a2a_context_id")
	require.True(t, exists)
	require.NotEmpty(t, contextID)

	status, exists := receivedMsg.MetaGetMut("a2a_status")
	require.True(t, exists)
	require.NotEmpty(t, status)

	t.Logf("Received response: %s", string(responseBytes))
	t.Logf("Task ID: %s", taskID)
	t.Logf("Status: %s", status)
}
