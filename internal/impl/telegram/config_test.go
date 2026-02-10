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
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateBotToken(t *testing.T) {
	tests := []struct {
		name        string
		token       string
		errContains string
	}{
		{
			name:        "valid token",
			token:       "123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
			errContains: "",
		},
		{
			name:        "valid token with underscores",
			token:       "987654321:ABC_def_GHI_jkl",
			errContains: "",
		},
		{
			name:        "valid token with hyphens",
			token:       "111222333:ABC-def-GHI-jkl",
			errContains: "",
		},
		{
			name:        "empty token",
			token:       "",
			errContains: "bot_token is required",
		},
		{
			name:        "missing colon",
			token:       "123456789ABCdefGHIjklMNOpqrsTUVwxyz",
			errContains: "invalid bot token format",
		},
		{
			name:        "missing bot id",
			token:       ":ABCdefGHIjklMNOpqrsTUVwxyz",
			errContains: "invalid bot token format",
		},
		{
			name:        "missing hash",
			token:       "123456789:",
			errContains: "invalid bot token format",
		},
		{
			name:        "invalid characters",
			token:       "123456789:ABC def GHI",
			errContains: "invalid bot token format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBotToken(tt.token)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateParseMode(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		errContains string
	}{
		{
			name:        "empty (no parse mode)",
			mode:        "",
			errContains: "",
		},
		{
			name:        "Markdown",
			mode:        "Markdown",
			errContains: "",
		},
		{
			name:        "MarkdownV2",
			mode:        "MarkdownV2",
			errContains: "",
		},
		{
			name:        "HTML",
			mode:        "HTML",
			errContains: "",
		},
		{
			name:        "invalid mode",
			mode:        "XML",
			errContains: "invalid parse_mode",
		},
		{
			name:        "lowercase markdown",
			mode:        "markdown",
			errContains: "invalid parse_mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateParseMode(tt.mode)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExtractChatID(t *testing.T) {
	tests := []struct {
		name        string
		setupMsg    func() *service.Message
		wantChatID  int64
		errContains string
	}{
		{
			name: "from metadata",
			setupMsg: func() *service.Message {
				msg := service.NewMessage([]byte("test"))
				msg.MetaSet("chat_id", "123456789")
				return msg
			},
			wantChatID:  123456789,
			errContains: "",
		},
		{
			name: "from message.chat.id structure",
			setupMsg: func() *service.Message {
				data := map[string]any{
					"message": map[string]any{
						"chat": map[string]any{
							"id": float64(987654321),
						},
					},
				}
				msg := service.NewMessage(nil)
				msg.SetStructured(data)
				return msg
			},
			wantChatID:  987654321,
			errContains: "",
		},
		{
			name: "from direct chat_id field",
			setupMsg: func() *service.Message {
				data := map[string]any{
					"chat_id": float64(555666777),
				}
				msg := service.NewMessage(nil)
				msg.SetStructured(data)
				return msg
			},
			wantChatID:  555666777,
			errContains: "",
		},
		{
			name: "missing chat_id",
			setupMsg: func() *service.Message {
				data := map[string]any{
					"other_field": "value",
				}
				msg := service.NewMessage(nil)
				msg.SetStructured(data)
				return msg
			},
			errContains: "chat_id not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.setupMsg()
			chatID, err := extractChatID(msg)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantChatID, chatID)
			}
		})
	}
}
