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
	"encoding/json"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/slack-go/slack"
)

func init() {
	err := service.RegisterOutput("slack_post", outputSpec(), newOutput)
	if err != nil {
		panic(err)
	}
}

const (
	oFieldBotToken    = "bot_token"
	oFieldChannelID   = "channel_id"
	oFieldThreadTS    = "thread_ts"
	oFieldText        = "text"
	oFieldBlocks      = "blocks"
	oFieldMarkdown    = "markdown"
	oFieldUnfurlLinks = "unfurl_links"
	oFieldUnfurlMedia = "unfurl_media"
	oFieldLinkNames   = "link_names"
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Description(`Post a new message to a Slack channel using https://api.slack.com/methods/chat.postMessage[^chat.postMessage]`).
		Fields(
			service.NewStringField(oFieldBotToken).Description("The Slack Bot User OAuth token to use.").LintRule(`
        root = if !this.has_prefix("xoxb-") { [ "field must start with xoxb-" ] }
      `),
			service.NewInterpolatedStringField(oFieldChannelID).Description("The channel ID to post messages to."),
			service.NewInterpolatedStringField(oFieldThreadTS).Description("Optional thread timestamp to post messages to.").Default(slack.DEFAULT_MESSAGE_THREAD_TIMESTAMP),
			service.NewInterpolatedStringField(oFieldText).Description("The text content of the message. Mutually exclusive with `blocks`.").
				Default(""),
			service.NewBloblangField(oFieldBlocks).Description("A Bloblang query that should return a JSON array of Slack blocks (see https://api.slack.com/reference/block-kit/blocks[Blocks in Slack documentation]). Mutually exclusive with `text`.").
				Optional(),
			service.NewBoolField(oFieldMarkdown).Description("Enable markdown formatting in the message.").Default(slack.DEFAULT_MESSAGE_MARKDOWN),
			service.NewBoolField(oFieldUnfurlLinks).Description("Enable link unfurling in the message.").Default(slack.DEFAULT_MESSAGE_UNFURL_LINKS),
			service.NewBoolField(oFieldUnfurlMedia).Description("Enable media unfurling in the message.").Default(slack.DEFAULT_MESSAGE_UNFURL_MEDIA),
			service.NewBoolField(oFieldLinkNames).Description("Enable link names in the message.").Default(false),
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

func newOutput(conf *service.ParsedConfig, res *service.Resources) (service.Output, int, error) {
	botToken, err := conf.FieldString(oFieldBotToken)
	if err != nil {
		return nil, 0, err
	}
	channelID, err := conf.FieldInterpolatedString(oFieldChannelID)
	if err != nil {
		return nil, 0, err
	}
	threadTS, err := conf.FieldInterpolatedString(oFieldThreadTS)
	if err != nil {
		return nil, 0, err
	}
	var text *service.InterpolatedString
	var blocks *bloblang.Executor
	if conf.Contains(oFieldBlocks) {
		blocks, err = conf.FieldBloblang(oFieldBlocks)
		if err != nil {
			return nil, 0, err
		}
	} else {
		text, err = conf.FieldInterpolatedString(oFieldText)
		if err != nil {
			return nil, 0, err
		}
	}
	markdown, err := conf.FieldBool(oFieldMarkdown)
	if err != nil {
		return nil, 0, err
	}
	unfurlLinks, err := conf.FieldBool(oFieldUnfurlLinks)
	if err != nil {
		return nil, 0, err
	}
	unfurlMedia, err := conf.FieldBool(oFieldUnfurlMedia)
	if err != nil {
		return nil, 0, err
	}
	linkNames, err := conf.FieldBool(oFieldLinkNames)
	if err != nil {
		return nil, 0, err
	}

	return &postOutput{
		api:         slack.New(botToken),
		channelID:   channelID,
		threadTS:    threadTS,
		text:        text,
		blocks:      blocks,
		markdown:    markdown,
		unfurlLinks: unfurlLinks,
		unfurlMedia: unfurlMedia,
		linkNames:   linkNames,
	}, 1, err
}

type postOutput struct {
	api       *slack.Client
	channelID *service.InterpolatedString
	threadTS  *service.InterpolatedString

	text        *service.InterpolatedString
	blocks      *bloblang.Executor
	markdown    bool
	unfurlLinks bool
	unfurlMedia bool
	linkNames   bool
}

var _ service.Output = (*postOutput)(nil)

// Connect implements service.Output.
func (o *postOutput) Connect(ctx context.Context) error {
	_, err := o.api.AuthTestContext(ctx)
	return err
}

// Write implements service.Output.
func (o *postOutput) Write(ctx context.Context, msg *service.Message) error {
	channelID, err := o.channelID.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate channel ID: %w", err)
	}
	options := []slack.MsgOption{}
	ts, err := o.threadTS.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate thread ID: %w", err)
	}
	if ts != "" {
		options = append(options, slack.MsgOptionTS(ts))
	}
	if o.blocks != nil {
		q, err := msg.BloblangQuery(o.blocks)
		if err != nil {
			return fmt.Errorf("failed to process blocks: %w", err)
		}
		b, err := q.AsBytes()
		if err != nil {
			return fmt.Errorf("failed to serialize blocks as JSON: %w", err)
		}
		var blocks slack.Blocks
		if err = json.Unmarshal(b, &blocks); err != nil {
			return fmt.Errorf("failed to unmarshal blocks: %w", err)
		}
		options = append(options, slack.MsgOptionBlocks(blocks.BlockSet...))
	} else {
		text, err := o.text.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to interpolate text: %w", err)
		}
		options = append(options, slack.MsgOptionText(text, false))
	}
	if !o.markdown {
		options = append(options, slack.MsgOptionDisableMarkdown())
	}
	if !o.unfurlLinks {
		options = append(options, slack.MsgOptionDisableLinkUnfurl())
	}
	if !o.unfurlMedia {
		options = append(options, slack.MsgOptionDisableMediaUnfurl())
	}
	options = append(options, slack.MsgOptionLinkNames(o.linkNames))
	_, _, err = o.api.PostMessageContext(
		ctx,
		channelID,
		options...,
	)
	return err
}

// Close implements service.Output.
func (o *postOutput) Close(ctx context.Context) error {
	return nil
}
