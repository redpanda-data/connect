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
	"strconv"
	"strings"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func outputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("4.80.0").
		Categories("Services").
		Summary("Sends messages to Telegram chats.").
		Description(`
This output sends messages to Telegram chats using a bot token.
You must create a bot via @BotFather on Telegram and obtain a bot token.

The chat_id and text fields support interpolation, allowing you to dynamically
set the target chat and message content from the processed message.

## Authentication

Create a bot:
1. Open Telegram and search for @BotFather
2. Send /newbot and follow the prompts
3. Copy the bot token (format: 123456789:ABCdefGHIjklMNO...)

## Getting Chat IDs

To send messages, you need the chat ID:
- For users: Have them send a message to your bot, then check the input logs
- Use @userinfobot to get your user ID
- For groups: Add the bot to the group, send a message, check logs

## Rate Limits

- Default: 30 messages/second
- Groups: 20 messages/minute per group
- Per-chat: 1 message/second
- 429 errors indicate rate limit exceeded
`).
		Fields(
			service.NewStringField("bot_token").
				Description("The bot token obtained from @BotFather.").
				Secret().
				Example("123456789:ABCdefGHIjklMNOpqrsTUVwxyz"),
			service.NewInterpolatedStringField("chat_id").
				Description("The chat ID to send the message to. Supports interpolation.").
				Example("${!json(\"chat_id\")}").
				Example("${!json(\"message.chat.id\")}").
				Example("123456789"),
			service.NewInterpolatedStringField("text").
				Description("The message text to send. Supports interpolation.").
				Example("${!content()}").
				Example("Alert: ${!json(\"alert_message\")}"),
			service.NewStringField("parse_mode").
				Description("The parse mode for the message text.").
				Optional().
				Example("Markdown").
				Example("MarkdownV2").
				Example("HTML").
				Advanced(),
			service.NewBoolField("disable_notification").
				Description("Send the message silently (users will receive a notification with no sound).").
				Default(false).
				Advanced(),
		)
}

func init() {
	err := service.RegisterOutput("telegram", outputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
		out, err := newTelegramOutput(conf, mgr)
		return out, 1, err
	})
	if err != nil {
		panic(err)
	}
}

type telegramOutput struct {
	botToken           string
	chatID             *service.InterpolatedString
	text               *service.InterpolatedString
	parseMode          string
	disableNotification bool

	log *service.Logger
	bot *bot.Bot
}

func newTelegramOutput(conf *service.ParsedConfig, mgr *service.Resources) (*telegramOutput, error) {
	botToken, err := conf.FieldString("bot_token")
	if err != nil {
		return nil, err
	}

	if err := validateBotToken(botToken); err != nil {
		return nil, err
	}

	chatID, err := conf.FieldInterpolatedString("chat_id")
	if err != nil {
		return nil, err
	}

	text, err := conf.FieldInterpolatedString("text")
	if err != nil {
		return nil, err
	}

	var parseMode string
	if conf.Contains("parse_mode") {
		parseMode, err = conf.FieldString("parse_mode")
		if err != nil {
			return nil, err
		}
		if err := validateParseMode(parseMode); err != nil {
			return nil, err
		}
	}

	disableNotification, err := conf.FieldBool("disable_notification")
	if err != nil {
		return nil, err
	}

	return &telegramOutput{
		botToken:            botToken,
		chatID:              chatID,
		text:                text,
		parseMode:           parseMode,
		disableNotification: disableNotification,
		log:                 mgr.Logger(),
	}, nil
}

func (t *telegramOutput) Connect(ctx context.Context) error {
	b, err := bot.New(t.botToken)
	if err != nil {
		return fmt.Errorf("creating telegram bot: %w", err)
	}
	t.bot = b

	// Validate the bot token by calling GetMe
	me, err := b.GetMe(ctx)
	if err != nil {
		return fmt.Errorf("validating bot token (check token and network): %w", err)
	}

	t.log.Infof("Connected to Telegram as @%s (ID: %d)", me.Username, me.ID)

	return nil
}

func (t *telegramOutput) Write(ctx context.Context, msg *service.Message) error {
	// Interpolate chat_id from message
	chatIDStr, err := t.chatID.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate chat_id: %w", err)
	}

	chatID, err := strconv.ParseInt(chatIDStr, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing chat_id '%s' as numeric ID: %w", chatIDStr, err)
	}

	// Interpolate text from message
	text, err := t.text.TryString(msg)
	if err != nil {
		return fmt.Errorf("interpolating message text: %w", err)
	}

	if text == "" {
		return fmt.Errorf("message text is empty")
	}

	// Send message
	params := &bot.SendMessageParams{
		ChatID:              chatID,
		Text:                text,
		DisableNotification: t.disableNotification,
	}

	if t.parseMode != "" {
		params.ParseMode = models.ParseMode(t.parseMode)
	}

	_, err = t.bot.SendMessage(ctx, params)
	if err != nil {
		// Provide helpful error messages for common issues
		errStr := err.Error()
		if strings.Contains(errStr, "chat not found") {
			return fmt.Errorf("sending message to chat_id %d (user must start chat with bot first): %w", chatID, err)
		}
		if strings.Contains(errStr, "429") || strings.Contains(errStr, "Too Many Requests") {
			return fmt.Errorf("sending message (rate limit exceeded - max 30 msg/sec, 1 msg/sec per chat): %w", err)
		}
		if strings.Contains(errStr, "Forbidden") {
			return fmt.Errorf("sending message (bot blocked by user or removed from chat): %w", err)
		}
		return fmt.Errorf("sending message to telegram: %w", err)
	}

	return nil
}

func (t *telegramOutput) Close(ctx context.Context) error {
	t.log.Debug("Telegram bot output closed")
	return nil
}
