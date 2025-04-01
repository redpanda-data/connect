/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package slack

import (
	"context"
	"errors"
	"fmt"

	"github.com/Jeffail/shutdown"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

func init() {
	err := service.RegisterInput("slack", inputSpec(), newInput)
	if err != nil {
		panic(err)
	}
}

const (
	iFieldAppToken = "app_token"
	iFieldBotToken = "bot_token"
)

func inputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Description(`Connects to Slack using https://api.slack.com/apis/socket-mode[^Socket Mode]. This allows for receiving events, interactions and slash commands. Each message emitted from this input has a @type metadata of the event type "events_api", "interactions" or "slash_commands".`).
		Fields(
			service.NewStringField(iFieldAppToken).Description("The Slack App token to use.").LintRule(`
        root = if !this.has_prefix("xapp-") { [ "field must start with xapp-" ] }
      `),
			service.NewStringField(iFieldBotToken).Description("The Slack Bot User OAuth token to use.").LintRule(`
        root = if !this.has_prefix("xoxb-") { [ "field must start with xoxb-" ] }
      `),
			service.NewAutoRetryNacksToggleField(),
		).
		Example("Echo Slackbot", "A slackbot that echo messages from other users", `
input:
  slack:
    app_token: "${APP_TOKEN:xapp-demo}"
    bot_token: "${BOT_TOKEN:xoxb-demo}"
pipeline:
  processors:
    - mutation: |
        # ignore hidden or non message events
        if this.event.type != "message" || (this.event.hidden | false) {
          root = deleted()
        }
        # Don't respond to our own messages
        if this.authorizations.any(auth -> auth.user_id == this.event.user) {
          root = deleted()
        }
output:
  slack_post:
    bot_token: "${BOT_TOKEN:xoxb-demo}"
    channel_id: "${!this.event.channel}"
    thread_ts: "${!this.event.ts}"
    text: "ECHO: ${!this.event.text}"
    `)
}

func newInput(conf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
	appToken, err := conf.FieldString(iFieldAppToken)
	if err != nil {
		return nil, err
	}
	botToken, err := conf.FieldString(iFieldBotToken)
	if err != nil {
		return nil, err
	}
	return service.AutoRetryNacksToggled(conf, &input{
		appToken: appToken,
		botToken: botToken,
		log:      res.Logger(),
	})
}

type input struct {
	appToken string
	botToken string
	log      *service.Logger

	shutSig *shutdown.Signaller
	client  *socketmode.Client
}

func (i *input) Connect(ctx context.Context) error {
	api := slack.New(i.botToken, slack.OptionAppLevelToken(i.appToken))
	client := socketmode.New(api)
	shutSig := shutdown.NewSignaller()
	go func() {
		defer shutSig.TriggerHasStopped()
		ctx, cancel := shutSig.HardStopCtx(context.Background())
		defer cancel()
		err := client.RunContext(ctx)
		if err != nil && !errors.Is(err, ctx.Err()) {
			i.log.Warnf("error running: %v", err)
		}
	}()
	i.client = client
	i.shutSig = shutSig
	return nil
}

func (i *input) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for {
		select {
		case evt, ok := <-i.client.Events:
			if !ok {
				return nil, nil, service.ErrNotConnected
			}
			switch evt.Type {
			case socketmode.EventTypeConnected,
				socketmode.EventTypeConnecting:
				i.log.Debugf("%v to slack", evt.Type)
				continue
			case socketmode.EventTypeInvalidAuth,
				socketmode.EventTypeConnectionError,
				socketmode.EventTypeIncomingError,
				socketmode.EventTypeErrorBadMessage,
				socketmode.EventTypeErrorWriteFailed:
				return nil, nil, fmt.Errorf("unexpected error event to slack: %v", evt.Type)
			case socketmode.EventTypeHello, socketmode.EventTypeDisconnect:
				i.log.Debugf("%v message from slack", evt.Type)
				continue
			case socketmode.EventTypeEventsAPI,
				socketmode.EventTypeInteractive,
				socketmode.EventTypeSlashCommand:
				// These are the messages we want and need to ack
				break
			}
			msg := service.NewMessage(evt.Request.Payload)
			msg.MetaSetMut("type", string(evt.Type))
			return msg, func(ctx context.Context, err error) error {
				if i.client == nil {
					return nil
				}
				return i.client.AckCtx(ctx, evt.Request.EnvelopeID, nil)
			}, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-i.shutSig.HasStoppedChan():
			return nil, nil, service.ErrNotConnected
		}
	}
}

func (i *input) Close(ctx context.Context) error {
	if i.client == nil {
		return nil
	}
	i.shutSig.TriggerHardStop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.shutSig.HasStoppedChan():
		return nil
	}
}
