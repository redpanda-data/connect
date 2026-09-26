// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package slack

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/slack-go/slack"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	service.MustRegisterProcessor("slack_thread", threadProcessorSpec(), newThreadProcessor)
}

const (
	pFieldBotToken  = "bot_token"
	pFieldChannelID = "channel_id"
	pFieldThreadTS  = "thread_ts"
)

func threadProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Reads a Slack thread using the Slack API method conversations.replies.").
		Description(`This processor calls the https://api.slack.com/methods/conversations.replies[`+"`conversations.replies`"+`^] Slack API method and replaces the message content with a JSON array of all messages in the thread.`).
		Fields(
			service.NewStringField(pFieldBotToken).Description("Your Slack bot user's OAuth token, which must have the correct permissions to read messages from the Slack channel specified in `channel_id`.").LintRule(`
        root = if !this.has_prefix("xoxb-") { [ "field must start with xoxb-" ] }
      `),
			service.NewInterpolatedStringField(pFieldChannelID).Description("The encoded ID of the Slack channel from which to read threads."),
			service.NewInterpolatedStringField(pFieldThreadTS).Description("The timestamp of the parent message of the thread you want to read."),
		)
}

func newThreadProcessor(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
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
		return nil, fmt.Errorf("interpolating channel ID: %w", err)
	}
	threadTS, err := t.threadTS.TryString(m)
	if err != nil {
		return nil, fmt.Errorf("interpolating thread timestamp: %w", err)
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
			return nil, fmt.Errorf("getting conversation replies: %w", err)
		}
		thread = append(thread, msgs...)
	}
	msg := m.Copy()
	b, err := json.Marshal(thread)
	if err != nil {
		return nil, fmt.Errorf("marshalling thread: %w", err)
	}
	msg.SetBytes(b)
	return service.MessageBatch{msg}, nil
}

// Close implements service.Processor.
func (*threadProcessor) Close(context.Context) error {
	return nil
}
