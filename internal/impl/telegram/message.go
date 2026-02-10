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
	"fmt"
	"time"

	"github.com/go-telegram/bot/models"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// parseUpdate converts a Telegram Update into a Benthos message.
// The message content is the JSON-serialized Update object.
// Metadata includes chat_id, user_id, message_id, and timestamp for easy access.
func parseUpdate(update *models.Update) (*service.Message, error) {
	if update == nil {
		return nil, fmt.Errorf("nil update")
	}

	// Serialize the entire update as JSON
	content, err := json.Marshal(update)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize update: %w", err)
	}

	msg := service.NewMessage(content)

	// Extract and set metadata from the update
	extractMetadata(update, msg)

	return msg, nil
}

// extractMetadata extracts common fields from the Update and sets them as message metadata.
func extractMetadata(update *models.Update, msg *service.Message) {
	// Set the update ID
	msg.MetaSet("update_id", fmt.Sprintf("%d", update.ID))

	// Extract metadata based on update type
	var chatID int64
	var userID int64
	var messageID int
	var timestamp time.Time
	var messageType string

	switch {
	case update.Message != nil:
		messageType = "message"
		chatID = update.Message.Chat.ID
		if update.Message.From != nil {
			userID = update.Message.From.ID
		}
		messageID = update.Message.ID
		timestamp = time.Unix(update.Message.Date, 0)

	case update.EditedMessage != nil:
		messageType = "edited_message"
		chatID = update.EditedMessage.Chat.ID
		if update.EditedMessage.From != nil {
			userID = update.EditedMessage.From.ID
		}
		messageID = update.EditedMessage.ID
		timestamp = time.Unix(update.EditedMessage.Date, 0)

	case update.ChannelPost != nil:
		messageType = "channel_post"
		chatID = update.ChannelPost.Chat.ID
		if update.ChannelPost.From != nil {
			userID = update.ChannelPost.From.ID
		}
		messageID = update.ChannelPost.ID
		timestamp = time.Unix(update.ChannelPost.Date, 0)

	case update.EditedChannelPost != nil:
		messageType = "edited_channel_post"
		chatID = update.EditedChannelPost.Chat.ID
		if update.EditedChannelPost.From != nil {
			userID = update.EditedChannelPost.From.ID
		}
		messageID = update.EditedChannelPost.ID
		timestamp = time.Unix(update.EditedChannelPost.Date, 0)

	case update.CallbackQuery != nil:
		messageType = "callback_query"
		if update.CallbackQuery.Message != nil {
			if msg := update.CallbackQuery.Message.Message; msg != nil {
				chatID = msg.Chat.ID
				messageID = msg.ID
			}
		}
		userID = update.CallbackQuery.From.ID
		timestamp = time.Now()

	case update.InlineQuery != nil:
		messageType = "inline_query"
		userID = update.InlineQuery.From.ID
		timestamp = time.Now()

	default:
		messageType = "unknown"
		timestamp = time.Now()
	}

	// Set metadata
	if chatID != 0 {
		msg.MetaSet("chat_id", fmt.Sprintf("%d", chatID))
	}
	if userID != 0 {
		msg.MetaSet("user_id", fmt.Sprintf("%d", userID))
	}
	if messageID != 0 {
		msg.MetaSet("message_id", fmt.Sprintf("%d", messageID))
	}
	msg.MetaSet("message_type", messageType)
	msg.MetaSet("timestamp", timestamp.Format(time.RFC3339))
}
