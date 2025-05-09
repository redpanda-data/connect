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

	"github.com/slack-go/slack"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	err := service.RegisterProcessor("slack_thread", threadProcessorSpec(), newThreadProcessor)
	if err != nil {
		panic(err)
	}
}

const (
	pFieldBotToken  = "bot_token"
	pFieldChannelID = "channel_id"
	pFieldThreadTS  = "thread_ts"
)

func threadProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Description(`Read a thread using the https://api.slack.com/methods/conversations.replies[^Slack API]`).
		Fields(
			service.NewStringField(pFieldBotToken).Description("The Slack Bot User OAuth token to use.").LintRule(`
        root = if !this.has_prefix("xoxb-") { [ "field must start with xoxb-" ] }
      `),
			service.NewInterpolatedStringField(pFieldChannelID).Description("The channel ID to read messages from."),
			service.NewInterpolatedStringField(pFieldThreadTS).Description("The thread timestamp to read the full thread of."),
		)
}

func newThreadProcessor(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	botToken, err := conf.FieldString(pFieldBotToken)
	if err != nil {
		return nil, err
	}
	channelID, err := conf.FieldInterpolatedString(pFieldChannelID)
	if err != nil {
		return nil, err
	}
	threadTS, err := conf.FieldInterpolatedString(pFieldThreadTS)
	if err != nil {
		return nil, err
	}
	return &threadProcessor{
		client:    slack.New(botToken),
		channelID: channelID,
		threadTS:  threadTS,
	}, nil
}

type threadProcessor struct {
	client              *slack.Client
	channelID, threadTS *service.InterpolatedString
}

var _ service.Processor = (*threadProcessor)(nil)

// Process implements service.Processor.
func (t *threadProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	channelID, err := t.channelID.TryString(m)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate channel ID: %w", err)
	}
	threadTS, err := t.threadTS.TryString(m)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate thread timestamp: %w", err)
	}
	cursor := ""
	var thread []slack.Message
	hasMore := true
	for hasMore {
		var msgs []slack.Message
		msgs, hasMore, cursor, err = t.client.GetConversationRepliesContext(
			ctx,
			&slack.GetConversationRepliesParameters{
				ChannelID: channelID,
				Timestamp: threadTS,
				Cursor:    cursor,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get conversation replies: %w", err)
		}
		thread = append(thread, msgs...)
	}
	msg := m.Copy()
	b, err := json.Marshal(thread)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal thread: %w", err)
	}
	msg.SetBytes(b)
	return service.MessageBatch{msg}, nil
}

// Close implements service.Processor.
func (t *threadProcessor) Close(ctx context.Context) error {
	return nil
}
