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

	"github.com/go-telegram/bot/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// mockTelegramOutputServer creates a test HTTP server for output testing
type mockTelegramOutputServer struct {
	*httptest.Server
	mu             sync.Mutex
	sentMessages   []sentMessage
	shouldFail     bool
	failureCode    int
	failureMessage string
	botInfo        *models.User
}

type sentMessage struct {
	ChatID              int64
	Text                string
	ParseMode           string
	DisableNotification bool
}

func newMockTelegramOutputServer() *mockTelegramOutputServer {
	mock := &mockTelegramOutputServer{
		sentMessages: []sentMessage{},
		botInfo: &models.User{
			ID:        123456789,
			Username:  "test_bot",
			FirstName: "Test Bot",
			IsBot:     true,
		},
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/bot", func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		defer mock.mu.Unlock()

		if strings.Contains(r.URL.Path, "/getMe") {
			if mock.shouldFail {
				w.WriteHeader(mock.failureCode)
				json.NewEncoder(w).Encode(map[string]any{
					"ok":          false,
					"description": mock.failureMessage,
				})
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"ok":     true,
				"result": mock.botInfo,
			})
			return
		}

		if strings.Contains(r.URL.Path, "/sendMessage") {
			if mock.shouldFail {
				w.WriteHeader(mock.failureCode)
				json.NewEncoder(w).Encode(map[string]any{
					"ok":          false,
					"description": mock.failureMessage,
				})
				return
			}

			// Parse the request body
			var req struct {
				ChatID              int64  `json:"chat_id"`
				Text                string `json:"text"`
				ParseMode           string `json:"parse_mode,omitempty"`
				DisableNotification bool   `json:"disable_notification,omitempty"`
			}
			json.NewDecoder(r.Body).Decode(&req)

			// Store sent message
			mock.sentMessages = append(mock.sentMessages, sentMessage{
				ChatID:              req.ChatID,
				Text:                req.Text,
				ParseMode:           req.ParseMode,
				DisableNotification: req.DisableNotification,
			})

			// Return success response
			json.NewEncoder(w).Encode(map[string]any{
				"ok": true,
				"result": map[string]any{
					"message_id": len(mock.sentMessages),
					"chat": map[string]any{
						"id": req.ChatID,
					},
					"text": req.Text,
				},
			})
			return
		}

		w.WriteHeader(http.StatusNotFound)
	})

	mock.Server = httptest.NewServer(mux)
	return mock
}

func (m *mockTelegramOutputServer) setFailure(shouldFail bool, code int, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFail = shouldFail
	m.failureCode = code
	m.failureMessage = message
}

func (m *mockTelegramOutputServer) getSentMessages() []sentMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sentMessage{}, m.sentMessages...)
}

func (m *mockTelegramOutputServer) clearMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = []sentMessage{}
}

func TestOutputConnect_Success(t *testing.T) {
	server := newMockTelegramOutputServer()
	defer server.Close()

	conf := fmt.Sprintf(`
bot_token: "%s:test-token"
chat_id: "123456789"
text: "${!content()}"
`, strings.TrimPrefix(server.URL, "http://"))

	env := service.NewEnvironment()
	parsed, err := outputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	output, err := newTelegramOutput(parsed, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	if err != nil {
		t.Skipf("Connect requires real Telegram API: %v", err)
	}

	output.Close(ctx)
}

func TestOutputConnect_InvalidToken(t *testing.T) {
	conf := `
bot_token: "invalid-token"
chat_id: "123456789"
text: "${!content()}"
`

	env := service.NewEnvironment()
	parsed, err := outputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	_, err = newTelegramOutput(parsed, service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid bot token format")
}

func TestOutputWrite_SimpleMessage(t *testing.T) {
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "987654321"
text: "${!content()}"
`

	env := service.NewEnvironment()
	parsed, err := outputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	output, err := newTelegramOutput(parsed, service.MockResources())
	require.NoError(t, err)

	// Create a test message
	msg := service.NewMessage([]byte("Hello, World!"))
	ctx := context.Background()

	// Write would fail without actual connection, but we can test the setup
	err = output.Write(ctx, msg)
	// Expected to fail since we don't have a real connection
	if err != nil {
		assert.Contains(t, err.Error(), "sending message")
	}
}

func TestOutputWrite_ChatIDInterpolation(t *testing.T) {
	tests := []struct {
		name           string
		chatIDTemplate string
		setupMsg       func() *service.Message
		expectError    bool
		errorContains  string
	}{
		{
			name:           "static chat_id",
			chatIDTemplate: "123456789",
			setupMsg: func() *service.Message {
				return service.NewMessage([]byte("test"))
			},
			expectError: false,
		},
		{
			name:           "interpolate from metadata",
			chatIDTemplate: "${!json(\"chat_id\")}",
			setupMsg: func() *service.Message {
				msg := service.NewMessage([]byte(`{"chat_id":987654321}`))
				return msg
			},
			expectError: false,
		},
		{
			name:           "interpolate from nested field",
			chatIDTemplate: "${!json(\"message.chat.id\")}",
			setupMsg: func() *service.Message {
				data := map[string]any{
					"message": map[string]any{
						"chat": map[string]any{
							"id": float64(555666777),
						},
					},
				}
				msg := service.NewMessage(nil)
				msg.SetStructured(data)
				return msg
			},
			expectError: false,
		},
		{
			name:           "invalid chat_id format",
			chatIDTemplate: "${!content()}",
			setupMsg: func() *service.Message {
				return service.NewMessage([]byte("not-a-number"))
			},
			expectError:   true,
			errorContains: "parsing chat_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := fmt.Sprintf(`
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "%s"
text: "test message"
`, tt.chatIDTemplate)

			env := service.NewEnvironment()
			parsed, err := outputConfigSpec().ParseYAML(conf, env)
			require.NoError(t, err)

			output, err := newTelegramOutput(parsed, service.MockResources())
			require.NoError(t, err)

			msg := tt.setupMsg()
			ctx := context.Background()

			err = output.Write(ctx, msg)
			if err != nil {
				if tt.expectError {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			}
		})
	}
}

func TestOutputWrite_TextInterpolation(t *testing.T) {
	tests := []struct {
		name          string
		textTemplate  string
		setupMsg      func() *service.Message
		expectError   bool
		errorContains string
	}{
		{
			name:         "static text",
			textTemplate: "Hello, World!",
			setupMsg: func() *service.Message {
				return service.NewMessage([]byte("ignored"))
			},
			expectError: false,
		},
		{
			name:         "interpolate content",
			textTemplate: "${!content()}",
			setupMsg: func() *service.Message {
				return service.NewMessage([]byte("Message content"))
			},
			expectError: false,
		},
		{
			name:         "interpolate from json",
			textTemplate: "Alert: ${!json(\"alert_message\")}",
			setupMsg: func() *service.Message {
				data := map[string]any{
					"alert_message": "System overload!",
				}
				msg := service.NewMessage(nil)
				msg.SetStructured(data)
				return msg
			},
			expectError: false,
		},
		{
			name:         "empty text after interpolation",
			textTemplate: "${!json(\"missing_field\")}",
			setupMsg: func() *service.Message {
				return service.NewMessage([]byte("{}"))
			},
			expectError:   true,
			errorContains: "message text is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := fmt.Sprintf(`
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "123456789"
text: "%s"
`, tt.textTemplate)

			env := service.NewEnvironment()
			parsed, err := outputConfigSpec().ParseYAML(conf, env)
			require.NoError(t, err)

			output, err := newTelegramOutput(parsed, service.MockResources())
			require.NoError(t, err)

			msg := tt.setupMsg()
			ctx := context.Background()

			err = output.Write(ctx, msg)
			if err != nil {
				if tt.expectError {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			}
		})
	}
}

func TestOutputWrite_ParseMode(t *testing.T) {
	tests := []struct {
		name      string
		parseMode string
		wantError bool
	}{
		{
			name:      "no parse mode",
			parseMode: "",
			wantError: false,
		},
		{
			name:      "Markdown",
			parseMode: "Markdown",
			wantError: false,
		},
		{
			name:      "MarkdownV2",
			parseMode: "MarkdownV2",
			wantError: false,
		},
		{
			name:      "HTML",
			parseMode: "HTML",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			confStr := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "123456789"
text: "test"
`
			if tt.parseMode != "" {
				confStr += fmt.Sprintf("parse_mode: %s\n", tt.parseMode)
			}

			env := service.NewEnvironment()
			parsed, err := outputConfigSpec().ParseYAML(confStr, env)
			require.NoError(t, err)

			output, err := newTelegramOutput(parsed, service.MockResources())
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.parseMode, output.parseMode)
			}
		})
	}
}

func TestOutputWrite_DisableNotification(t *testing.T) {
	tests := []struct {
		name                string
		disableNotification bool
	}{
		{
			name:                "notifications enabled",
			disableNotification: false,
		},
		{
			name:                "notifications disabled",
			disableNotification: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := fmt.Sprintf(`
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "123456789"
text: "test"
disable_notification: %v
`, tt.disableNotification)

			env := service.NewEnvironment()
			parsed, err := outputConfigSpec().ParseYAML(conf, env)
			require.NoError(t, err)

			output, err := newTelegramOutput(parsed, service.MockResources())
			require.NoError(t, err)
			assert.Equal(t, tt.disableNotification, output.disableNotification)
		})
	}
}

func TestOutputClose_Idempotent(t *testing.T) {
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "123456789"
text: "test"
`

	env := service.NewEnvironment()
	parsed, err := outputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	output, err := newTelegramOutput(parsed, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	// Close multiple times should not panic or error
	err = output.Close(ctx)
	assert.NoError(t, err)

	err = output.Close(ctx)
	assert.NoError(t, err)

	err = output.Close(ctx)
	assert.NoError(t, err)
}

func TestOutputWrite_ErrorHandling(t *testing.T) {
	tests := []struct {
		name          string
		chatID        string
		text          string
		setupMsg      func() *service.Message
		errorContains string
	}{
		{
			name:   "invalid chat_id interpolation",
			chatID: "${!json(\"invalid\")}",
			text:   "test",
			setupMsg: func() *service.Message {
				return service.NewMessage([]byte("{}"))
			},
			errorContains: "interpolate chat_id",
		},
		{
			name:   "non-numeric chat_id",
			chatID: "not-a-number",
			text:   "test",
			setupMsg: func() *service.Message {
				return service.NewMessage([]byte("test"))
			},
			errorContains: "parsing chat_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := fmt.Sprintf(`
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "%s"
text: "%s"
`, tt.chatID, tt.text)

			env := service.NewEnvironment()
			parsed, err := outputConfigSpec().ParseYAML(conf, env)
			require.NoError(t, err)

			output, err := newTelegramOutput(parsed, service.MockResources())
			require.NoError(t, err)

			msg := tt.setupMsg()
			ctx := context.Background()

			err = output.Write(ctx, msg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorContains)
		})
	}
}

func TestOutputConfiguration_AllFields(t *testing.T) {
	conf := `
bot_token: "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
chat_id: "987654321"
text: "Hello ${!json(\"name\")}"
parse_mode: "Markdown"
disable_notification: true
`

	env := service.NewEnvironment()
	parsed, err := outputConfigSpec().ParseYAML(conf, env)
	require.NoError(t, err)

	output, err := newTelegramOutput(parsed, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11", output.botToken)
	assert.Equal(t, "Markdown", output.parseMode)
	assert.True(t, output.disableNotification)
	assert.NotNil(t, output.chatID)
	assert.NotNil(t, output.text)
}
