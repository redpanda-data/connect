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
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateBotToken(t *testing.T) {
	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{
			name:    "valid token",
			token:   "123456789:ABCdefGHIjklMNOpqrsTUVwxyz",
			wantErr: false,
		},
		{
			name:    "valid token with underscores",
			token:   "987654321:ABC_def_GHI_jkl",
			wantErr: false,
		},
		{
			name:    "valid token with hyphens",
			token:   "111222333:ABC-def-GHI-jkl",
			wantErr: false,
		},
		{
			name:    "empty token",
			token:   "",
			wantErr: true,
		},
		{
			name:    "missing colon",
			token:   "123456789ABCdefGHIjklMNOpqrsTUVwxyz",
			wantErr: true,
		},
		{
			name:    "missing bot id",
			token:   ":ABCdefGHIjklMNOpqrsTUVwxyz",
			wantErr: true,
		},
		{
			name:    "missing hash",
			token:   "123456789:",
			wantErr: true,
		},
		{
			name:    "invalid characters",
			token:   "123456789:ABC def GHI",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBotToken(tt.token)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateParseMode(t *testing.T) {
	tests := []struct {
		name    string
		mode    string
		wantErr bool
	}{
		{
			name:    "empty (no parse mode)",
			mode:    "",
			wantErr: false,
		},
		{
			name:    "Markdown",
			mode:    "Markdown",
			wantErr: false,
		},
		{
			name:    "MarkdownV2",
			mode:    "MarkdownV2",
			wantErr: false,
		},
		{
			name:    "HTML",
			mode:    "HTML",
			wantErr: false,
		},
		{
			name:    "invalid mode",
			mode:    "XML",
			wantErr: true,
		},
		{
			name:    "lowercase markdown",
			mode:    "markdown",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateParseMode(tt.mode)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExtractChatID(t *testing.T) {
	tests := []struct {
		name       string
		setupMsg   func() *service.Message
		wantChatID int64
		wantErr    bool
	}{
		{
			name: "from metadata",
			setupMsg: func() *service.Message {
				msg := service.NewMessage([]byte("test"))
				msg.MetaSet("chat_id", "123456789")
				return msg
			},
			wantChatID: 123456789,
			wantErr:    false,
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
			wantChatID: 987654321,
			wantErr:    false,
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
			wantChatID: 555666777,
			wantErr:    false,
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
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.setupMsg()
			chatID, err := extractChatID(msg)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantChatID, chatID)
			}
		})
	}
}
