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
	"fmt"
	"regexp"
	"strconv"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var botTokenRegex = regexp.MustCompile(`^\d+:[\w-]+$`)

// validateBotToken checks if the bot token matches the expected format.
// Telegram bot tokens format: <bot_id>:<hash>
// Example: 123456789:ABCdefGHIjklMNOpqrsTUVwxyz
func validateBotToken(token string) error {
	if token == "" {
		return fmt.Errorf("bot_token is required")
	}
	if !botTokenRegex.MatchString(token) {
		return fmt.Errorf("invalid bot token format (expected: <number>:<hash>)")
	}
	return nil
}

// validateParseMode checks if the parse mode is one of the supported values.
func validateParseMode(mode string) error {
	switch mode {
	case "", "Markdown", "MarkdownV2", "HTML":
		return nil
	default:
		return fmt.Errorf("invalid parse_mode: must be 'Markdown', 'MarkdownV2', or 'HTML'")
	}
}

// extractChatID extracts the chat ID from a Benthos message.
// It looks for the chat ID in the message metadata or structured content.
func extractChatID(msg *service.Message) (int64, error) {
	// Try to get chat_id from metadata
	if chatIDStr, exists := msg.MetaGet("chat_id"); exists {
		chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse chat_id from metadata: %w", err)
		}
		return chatID, nil
	}

	// Try to get from structured content
	chatIDVal, err := msg.AsStructured()
	if err != nil {
		return 0, fmt.Errorf("failed to extract chat_id: %w", err)
	}

	chatIDMap, ok := chatIDVal.(map[string]any)
	if !ok {
		return 0, fmt.Errorf("message is not a structured object")
	}

	// Check for message.chat.id structure
	if message, ok := chatIDMap["message"].(map[string]any); ok {
		if chat, ok := message["chat"].(map[string]any); ok {
			if id, ok := chat["id"].(float64); ok {
				return int64(id), nil
			}
		}
	}

	// Check for direct chat_id field
	if chatID, ok := chatIDMap["chat_id"].(float64); ok {
		return int64(chatID), nil
	}

	return 0, fmt.Errorf("chat_id not found in message")
}
