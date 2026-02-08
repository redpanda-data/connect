// Copyright 2025 Redpanda Data, Inc.
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

package telegram

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTelegramIntegration(t *testing.T) {
	integration.CheckSkip(t)

	botToken := os.Getenv("TELEGRAM_TEST_BOT_TOKEN")
	if botToken == "" {
		t.Skip("TELEGRAM_TEST_BOT_TOKEN not set, skipping integration test")
	}

	testChatID := os.Getenv("TELEGRAM_TEST_CHAT_ID")
	if testChatID == "" {
		t.Skip("TELEGRAM_TEST_CHAT_ID not set, skipping integration test")
	}

	t.Run("send_message", func(t *testing.T) {
		// Create output
		spec := outputConfigSpec()
		parsedConf, err := spec.ParseYAML(fmt.Sprintf(`
bot_token: %s
chat_id: %s
text: "Integration test message at ${!timestamp_unix()}"
`, botToken, testChatID), nil)
		require.NoError(t, err)

		output, _, err := spec.NewOutput(parsedConf, service.MockResources())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Connect
		err = output.Connect(ctx)
		require.NoError(t, err)
		defer output.Close(ctx)

		// Send message
		msg := service.NewMessage([]byte("test content"))
		err = output.Write(ctx, msg)
		assert.NoError(t, err)
	})

	t.Run("send_receive_cycle", func(t *testing.T) {
		// This test requires manual interaction:
		// 1. Start the input
		// 2. Send a message to the bot from Telegram
		// 3. Verify the message is received
		// 4. Send a reply back

		// Create input
		inputSpec := inputConfigSpec()
		inputConf, err := inputSpec.ParseYAML(fmt.Sprintf(`
bot_token: %s
polling_timeout: 5s
`, botToken), nil)
		require.NoError(t, err)

		input, err := inputSpec.NewInput(inputConf, service.MockResources())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Connect
		err = input.Connect(ctx)
		require.NoError(t, err)
		defer input.Close(ctx)

		t.Log("Telegram bot is now listening. Send a message to the bot within 30 seconds...")
		t.Log("To test this properly, send: 'test message'")

		// Try to read a message
		msg, ackFn, err := input.Read(ctx)
		if err == context.DeadlineExceeded {
			t.Skip("No message received within timeout - this is expected if no manual message was sent")
			return
		}
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Ack the message
		err = ackFn(ctx, nil)
		assert.NoError(t, err)

		// Verify metadata
		chatID, exists := msg.MetaGet("chat_id")
		assert.True(t, exists)
		assert.NotEmpty(t, chatID)

		updateID, exists := msg.MetaGet("update_id")
		assert.True(t, exists)
		assert.NotEmpty(t, updateID)

		t.Logf("Received message from chat_id: %s, update_id: %s", chatID, updateID)
	})
}

func TestTelegramOutputInterpolation(t *testing.T) {
	botToken := os.Getenv("TELEGRAM_TEST_BOT_TOKEN")
	if botToken == "" {
		t.Skip("TELEGRAM_TEST_BOT_TOKEN not set, skipping integration test")
	}

	testChatID := os.Getenv("TELEGRAM_TEST_CHAT_ID")
	if testChatID == "" {
		t.Skip("TELEGRAM_TEST_CHAT_ID not set, skipping integration test")
	}

	integration.CheckSkip(t)

	// Create output with interpolated fields
	spec := outputConfigSpec()
	parsedConf, err := spec.ParseYAML(fmt.Sprintf(`
bot_token: %s
chat_id: ${!json("target_chat")}
text: "Alert: ${!json("message")}"
parse_mode: Markdown
`, botToken), nil)
	require.NoError(t, err)

	output, _, err := spec.NewOutput(parsedConf, service.MockResources())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect
	err = output.Connect(ctx)
	require.NoError(t, err)
	defer output.Close(ctx)

	// Send message with structured data
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"target_chat": testChatID,
		"message":     "Integration test with *bold* text",
	})

	err = output.Write(ctx, msg)
	assert.NoError(t, err)
}
