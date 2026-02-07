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
	"encoding/json"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		name           string
		update         *models.Update
		wantErr        bool
		checkContent   func(t *testing.T, content []byte)
		checkMetadata  func(t *testing.T, msg *service.Message)
	}{
		{
			name:    "nil update",
			update:  nil,
			wantErr: true,
		},
		{
			name: "text message",
			update: &models.Update{
				ID: 123,
				Message: &models.Message{
					ID:   456,
					Date: time.Now().Unix(),
					Chat: models.Chat{
						ID: 789,
					},
					From: &models.User{
						ID:       111,
						Username: "testuser",
					},
					Text: "Hello, world!",
				},
			},
			wantErr: false,
			checkContent: func(t *testing.T, content []byte) {
				var update models.Update
				require.NoError(t, json.Unmarshal(content, &update))
				assert.Equal(t, int64(123), update.ID)
				assert.NotNil(t, update.Message)
				assert.Equal(t, "Hello, world!", update.Message.Text)
			},
			checkMetadata: func(t *testing.T, msg *service.Message) {
				updateID, exists := msg.MetaGet("update_id")
				assert.True(t, exists)
				assert.Equal(t, "123", updateID)

				chatID, exists := msg.MetaGet("chat_id")
				assert.True(t, exists)
				assert.Equal(t, "789", chatID)

				userID, exists := msg.MetaGet("user_id")
				assert.True(t, exists)
				assert.Equal(t, "111", userID)

				msgType, exists := msg.MetaGet("message_type")
				assert.True(t, exists)
				assert.Equal(t, "message", msgType)
			},
		},
		{
			name: "edited message",
			update: &models.Update{
				ID: 124,
				EditedMessage: &models.Message{
					ID:   457,
					Date: time.Now().Unix(),
					Chat: models.Chat{
						ID: 790,
					},
					From: &models.User{
						ID: 112,
					},
					Text: "Edited text",
				},
			},
			wantErr: false,
			checkMetadata: func(t *testing.T, msg *service.Message) {
				msgType, exists := msg.MetaGet("message_type")
				assert.True(t, exists)
				assert.Equal(t, "edited_message", msgType)

				chatID, exists := msg.MetaGet("chat_id")
				assert.True(t, exists)
				assert.Equal(t, "790", chatID)
			},
		},
		{
			name: "channel post",
			update: &models.Update{
				ID: 125,
				ChannelPost: &models.Message{
					ID:   458,
					Date: time.Now().Unix(),
					Chat: models.Chat{
						ID:   -1001234567890,
						Type: "channel",
					},
					Text: "Channel announcement",
				},
			},
			wantErr: false,
			checkMetadata: func(t *testing.T, msg *service.Message) {
				msgType, exists := msg.MetaGet("message_type")
				assert.True(t, exists)
				assert.Equal(t, "channel_post", msgType)

				chatID, exists := msg.MetaGet("chat_id")
				assert.True(t, exists)
				assert.Equal(t, "-1001234567890", chatID)
			},
		},
		{
			name: "callback query",
			update: &models.Update{
				ID: 126,
				CallbackQuery: &models.CallbackQuery{
					ID: "callback123",
					From: models.User{
						ID: 113,
					},
					Data: "button_clicked",
				},
			},
			wantErr: false,
			checkMetadata: func(t *testing.T, msg *service.Message) {
				msgType, exists := msg.MetaGet("message_type")
				assert.True(t, exists)
				assert.Equal(t, "callback_query", msgType)

				userID, exists := msg.MetaGet("user_id")
				assert.True(t, exists)
				assert.Equal(t, "113", userID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := parseUpdate(tt.update)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, msg)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, msg)

			// Check content if test provides a checker
			if tt.checkContent != nil {
				content, err := msg.AsBytes()
				require.NoError(t, err)
				tt.checkContent(t, content)
			}

			// Check metadata if test provides a checker
			if tt.checkMetadata != nil {
				tt.checkMetadata(t, msg)
			}
		})
	}
}

func TestExtractMetadata(t *testing.T) {
	now := time.Now().Unix()

	tests := []struct {
		name         string
		update       *models.Update
		wantMetadata map[string]string
	}{
		{
			name: "regular message with all fields",
			update: &models.Update{
				ID: 100,
				Message: &models.Message{
					ID:   200,
					Date: now,
					Chat: models.Chat{
						ID: 300,
					},
					From: &models.User{
						ID: 400,
					},
					Text: "test",
				},
			},
			wantMetadata: map[string]string{
				"update_id":    "100",
				"chat_id":      "300",
				"user_id":      "400",
				"message_id":   "200",
				"message_type": "message",
			},
		},
		{
			name: "message without from user",
			update: &models.Update{
				ID: 101,
				Message: &models.Message{
					ID:   201,
					Date: now,
					Chat: models.Chat{
						ID: 301,
					},
					From: nil,
					Text: "anonymous",
				},
			},
			wantMetadata: map[string]string{
				"update_id":    "101",
				"chat_id":      "301",
				"message_id":   "201",
				"message_type": "message",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := parseUpdate(tt.update)
			require.NoError(t, err)

			for key, expectedValue := range tt.wantMetadata {
				actualValue, exists := msg.MetaGet(key)
				assert.True(t, exists, "metadata key %s should exist", key)
				assert.Equal(t, expectedValue, actualValue, "metadata key %s", key)
			}

			// Check that timestamp exists and is valid
			timestamp, exists := msg.MetaGet("timestamp")
			assert.True(t, exists)
			_, err = time.Parse(time.RFC3339, timestamp)
			assert.NoError(t, err, "timestamp should be valid RFC3339")
		})
	}
}
