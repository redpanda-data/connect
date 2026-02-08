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
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/pure/shutdown"
)

const (
	tiFieldBotToken       = "bot_token"
	tiFieldPollingTimeout = "polling_timeout"
	tiFieldAllowedUpdates = "allowed_updates"
)

func inputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("4.80.0").
		Categories("Services").
		Summary("Receives messages from Telegram via long polling.").
		Description(`
This input receives messages, media, and updates from a Telegram bot using long polling.
You must create a bot via @BotFather on Telegram and obtain a bot token.

Messages are output as JSON containing the full Telegram Update object.
Metadata fields (chat_id, user_id, message_id, timestamp) are also set for easy access.

## Authentication

Create a bot:
1. Open Telegram and search for @BotFather
2. Send /newbot and follow the prompts
3. Copy the bot token (format: 123456789:ABCdefGHIjklMNO...)

## Rate Limits

- Default: 30 messages/second
- Groups: 20 messages/minute
- Per-chat: 1 message/second
`).
		Fields(
			service.NewStringField(tiFieldBotToken).
				Description("The bot token obtained from @BotFather.").
				Secret().
				Example("123456789:ABCdefGHIjklMNOpqrsTUVwxyz"),
			service.NewDurationField(tiFieldPollingTimeout).
				Description("The timeout for long polling requests.").
				Default("30s").
				Advanced(),
			service.NewStringListField(tiFieldAllowedUpdates).
				Description("List of update types to receive. Leave empty to receive all types.").
				Example([]string{"message", "edited_message", "channel_post"}).
				Optional().
				Advanced(),
		)
}

func init() {
	err := service.RegisterInput("telegram", inputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return newTelegramInput(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

type telegramInput struct {
	botToken       string
	pollingTimeout time.Duration
	allowedUpdates []string

	log     *service.Logger
	shutSig *shutdown.Signaller

	bot       *bot.Bot
	updatesCh chan *models.Update
	botCtx    context.Context
	botCancel context.CancelFunc
}

func newTelegramInput(conf *service.ParsedConfig, mgr *service.Resources) (*telegramInput, error) {
	botToken, err := conf.FieldString(tiFieldBotToken)
	if err != nil {
		return nil, err
	}

	if err := validateBotToken(botToken); err != nil {
		return nil, err
	}

	pollingTimeout, err := conf.FieldDuration(tiFieldPollingTimeout)
	if err != nil {
		return nil, err
	}

	var allowedUpdates []string
	if conf.Contains(tiFieldAllowedUpdates) {
		allowedUpdates, err = conf.FieldStringList(tiFieldAllowedUpdates)
		if err != nil {
			return nil, err
		}
	}

	return &telegramInput{
		botToken:       botToken,
		pollingTimeout: pollingTimeout,
		allowedUpdates: allowedUpdates,
		log:            mgr.Logger(),
		shutSig:        shutdown.NewSignaller(),
		updatesCh:      make(chan *models.Update, 100),
	}, nil
}

func (t *telegramInput) Connect(ctx context.Context) error {
	opts := []bot.Option{
		bot.WithDefaultHandler(t.handleUpdate),
	}

	b, err := bot.New(t.botToken, opts...)
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

	// Create context for bot lifecycle management
	t.botCtx, t.botCancel = context.WithCancel(context.Background())

	// Start polling in the background
	go t.bot.Start(t.botCtx)

	return nil
}

func (t *telegramInput) handleUpdate(ctx context.Context, b *bot.Bot, update *models.Update) {
	select {
	case t.updatesCh <- update:
		// Message queued successfully
	case <-ctx.Done():
		return
	case <-t.shutSig.HardStopChan():
		return
	default:
		// Channel full - log and drop message to prevent deadlock
		t.log.Warnf("Update channel full, dropping telegram update ID %d (backpressure)", update.ID)
	}
}

func (t *telegramInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case update := <-t.updatesCh:
		// Convert to Benthos message
		msg, err := parseUpdate(update)
		if err != nil {
			return nil, nil, fmt.Errorf("parsing telegram update: %w", err)
		}

		// Simple ack - no persistent state needed
		ackFn := func(context.Context, error) error {
			return nil
		}

		return msg, ackFn, nil

	case <-ctx.Done():
		return nil, nil, ctx.Err()

	case <-t.shutSig.SoftStopChan():
		return nil, nil, service.ErrEndOfInput
	}
}

func (t *telegramInput) Close(ctx context.Context) error {
	t.shutSig.TriggerHardStop()

	// Cancel the bot context to stop polling
	if t.botCancel != nil {
		t.botCancel()
		t.botCancel = nil
	}

	t.log.Debug("Telegram bot input closed")
	return nil
}
