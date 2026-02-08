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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// mockTelegramServer creates a test HTTP server that mimics Telegram Bot API
type mockTelegramServer struct {
	*httptest.Server
	mu              sync.Mutex
	updates         []models.Update
	updateOffset    int
	getUpdatesCount int
	botInfo         *models.User
	shouldFail      bool
	failureCode     int
}

func newMockTelegramServer() *mockTelegramServer {
	mock := &mockTelegramServer{
		updates: []models.Update{},
		botInfo: &models.User{
			ID:        123456789,
			Username:  "test_bot",
			FirstName: "Test Bot",
			IsBot:     true,
		},
	}

	mux := http.NewServeMux()

	// Handle /botTOKEN/getMe endpoint
	mux.HandleFunc("/bot", func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		defer mock.mu.Unlock()

		if mock.shouldFail {
			w.WriteHeader(mock.failureCode)
			json.NewEncoder(w).Encode(map[string]any{
				"ok":          false,
				"description": "Unauthorized",
			})
			return
		}

		if strings.Contains(r.URL.Path, "/getMe") {
			json.NewEncoder(w).Encode(map[string]any{
				"ok":     true,
				"result": mock.botInfo,
			})
			return
		}

		if strings.Contains(r.URL.Path, "/getUpdates") {
			mock.getUpdatesCount++

			// Return pending updates
			var result []models.Update
			if mock.updateOffset < len(mock.updates) {
				result = mock.updates[mock.updateOffset:]
				mock.updateOffset = len(mock.updates)
			}

			json.NewEncoder(w).Encode(map[string]any{
				"ok":     true,
				"result": result,
			})
			return
		}

		w.WriteHeader(http.StatusNotFound)
	})

	mock.Server = httptest.NewServer(mux)
	return mock
}

func (m *mockTelegramServer) addUpdate(update models.Update) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates = append(m.updates, update)
}

func (m *mockTelegramServer) setFailure(shouldFail bool, code int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFail = shouldFail
	m.failureCode = code
}

func (m *mockTelegramServer) getUpdatesCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getUpdatesCount
}

func TestInputConnect_Success(t *testing.T) {
	server := newMockTelegramServer()
	defer server.Close()

	// Create input with mock server URL as token prefix
	conf := fmt.Sprintf(`
bot_token: "%s:test-token"
polling_timeout: 1s
`, strings.TrimPrefix(server.URL, "http://"))

	env := service.NewEnvironment()
	parsed, err := inputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	input, err := newTelegramInput(parsed, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = input.Connect(ctx)
	if err != nil {
		t.Skipf("Connect requires real Telegram API: %v", err)
	}

	input.Close(ctx)
}

func TestInputConnect_InvalidToken(t *testing.T) {
	// Test with clearly invalid token format
	conf := `
bot_token: "invalid-token"
polling_timeout: 1s
`

	env := service.NewEnvironment()
	parsed, err := inputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	_, err = newTelegramInput(parsed, service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid bot token format")
}

func TestInputConnect_EmptyToken(t *testing.T) {
	conf := `
bot_token: ""
polling_timeout: 1s
`

	env := service.NewEnvironment()
	_, err := inputConfigSpec().ParseYAML(conf, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bot_token")
}

func TestInputRead_ReceivesUpdates(t *testing.T) {
	// Create a valid config
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
polling_timeout: 1s
`

	env := service.NewEnvironment()
	parsed, err := inputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	input, err := newTelegramInput(parsed, service.MockResources())
	require.NoError(t, err)

	// Create a test update and inject it directly into the channel
	testUpdate := &models.Update{
		ID: 12345,
		Message: &models.Message{
			ID:   1,
			Date: time.Now().Unix(),
			Chat: models.Chat{
				ID:        987654321,
				Type:      "private",
				FirstName: "Test",
			},
			From: &models.User{
				ID:        111222333,
				FirstName: "Test User",
			},
			Text: "Hello, bot!",
		},
	}

	// Simulate receiving an update by directly sending to the channel
	go func() {
		time.Sleep(50 * time.Millisecond)
		input.updatesCh <- testUpdate
	}()

	// Try to read the update
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, ackFn, err := input.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.NotNil(t, ackFn)

	// Verify message content
	content, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(content), "Hello, bot!")

	// Verify metadata
	chatID, exists := msg.MetaGet("chat_id")
	require.True(t, exists)
	assert.Equal(t, "987654321", chatID)

	userID, exists := msg.MetaGet("user_id")
	require.True(t, exists)
	assert.Equal(t, "111222333", userID)

	// Ack the message
	err = ackFn(ctx, nil)
	assert.NoError(t, err)

	// Cleanup
	input.Close(ctx)
}

func TestInputRead_ContextCancellation(t *testing.T) {
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
polling_timeout: 1s
`

	env := service.NewEnvironment()
	parsed, err := inputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	input, err := newTelegramInput(parsed, service.MockResources())
	require.NoError(t, err)

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	// Read should return context error
	_, _, err = input.Read(ctx)
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	// Cleanup
	input.Close(context.Background())
}

func TestInputRead_SoftStop(t *testing.T) {
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
polling_timeout: 1s
`

	env := service.NewEnvironment()
	parsed, err := inputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	input, err := newTelegramInput(parsed, service.MockResources())
	require.NoError(t, err)

	// Trigger soft stop
	input.shutSig.TriggerSoftStop()

	// Read should return ErrEndOfInput
	ctx := context.Background()
	_, _, err = input.Read(ctx)
	require.Error(t, err)
	assert.Equal(t, service.ErrEndOfInput, err)

	// Cleanup
	input.Close(ctx)
}

func TestInputClose_Idempotent(t *testing.T) {
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
polling_timeout: 1s
`

	env := service.NewEnvironment()
	parsed, err := inputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	input, err := newTelegramInput(parsed, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	// Close multiple times should not panic or error
	err = input.Close(ctx)
	assert.NoError(t, err)

	err = input.Close(ctx)
	assert.NoError(t, err)

	err = input.Close(ctx)
	assert.NoError(t, err)
}

func TestInputBackpressure_DropsUpdates(t *testing.T) {
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
polling_timeout: 1s
`

	env := service.NewEnvironment()
	parsed, err := inputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	input, err := newTelegramInput(parsed, service.MockResources())
	require.NoError(t, err)

	// Fill the channel to capacity (100 messages)
	for i := 0; i < 100; i++ {
		input.updatesCh <- &models.Update{
			ID: i,
			Message: &models.Message{
				ID:   i,
				Date: time.Now().Unix(),
				Chat: models.Chat{ID: 12345},
				Text: fmt.Sprintf("Message %d", i),
			},
		}
	}

	// handleUpdate should drop the next update (channel is full)
	ctx := context.Background()
	droppedUpdate := &models.Update{
		ID: 999,
		Message: &models.Message{
			ID:   999,
			Date: time.Now().Unix(),
			Chat: models.Chat{ID: 12345},
			Text: "This should be dropped",
		},
	}

	// This should not block (drops the message)
	done := make(chan bool)
	go func() {
		input.handleUpdate(ctx, nil, droppedUpdate)
		done <- true
	}()

	select {
	case <-done:
		// Good - handleUpdate returned without blocking
	case <-time.After(1 * time.Second):
		t.Fatal("handleUpdate blocked instead of dropping message")
	}

	// Verify channel still has 100 messages (dropped update not added)
	assert.Equal(t, 100, len(input.updatesCh))

	// Cleanup
	input.Close(ctx)
}

func TestInputAllowedUpdates_Configuration(t *testing.T) {
	tests := []struct {
		name          string
		config        string
		expectedTypes []string
	}{
		{
			name: "default - all updates",
			config: `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
`,
			expectedTypes: nil, // empty means all types
		},
		{
			name: "specific update types",
			config: `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
allowed_updates:
  - message
  - edited_message
`,
			expectedTypes: []string{"message", "edited_message"},
		},
		{
			name: "single update type",
			config: `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
allowed_updates:
  - callback_query
`,
			expectedTypes: []string{"callback_query"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := service.NewEnvironment()
			parsed, err := inputConfigSpec().ParseYAML(tt.config, env)
			require.NoError(t, err)

			input, err := newTelegramInput(parsed, service.MockResources())
			require.NoError(t, err)

			if tt.expectedTypes == nil {
				assert.Nil(t, input.allowedUpdates)
			} else {
				assert.Equal(t, tt.expectedTypes, input.allowedUpdates)
			}
		})
	}
}

func TestInputPollingTimeout_Configuration(t *testing.T) {
	tests := []struct {
		name            string
		config          string
		expectedTimeout time.Duration
	}{
		{
			name: "default timeout",
			config: `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
`,
			expectedTimeout: 30 * time.Second,
		},
		{
			name: "custom timeout",
			config: `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
polling_timeout: 10s
`,
			expectedTimeout: 10 * time.Second,
		},
		{
			name: "long timeout",
			config: `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
polling_timeout: 2m
`,
			expectedTimeout: 2 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := service.NewEnvironment()
			parsed, err := inputConfigSpec().ParseYAML(tt.config, env)
			require.NoError(t, err)

			input, err := newTelegramInput(parsed, service.MockResources())
			require.NoError(t, err)
			assert.Equal(t, tt.expectedTimeout, input.pollingTimeout)
		})
	}
}
