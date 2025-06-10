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
	"fmt"

	"github.com/slack-go/slack"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	service.MustRegisterOutput("slack_reaction", reactionSpec(), newReaction)
}

const (
	orFieldTimestamp = "timestamp"
	orFieldEmoji     = "emoji"
	orFieldAction    = "action"
)

func reactionSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Description(`Add or remove an emoji reaction to a Slack message using https://api.slack.com/methods/reactions.add[^reactions.add] and https://api.slack.com/methods/reactions.remove[^reactions.remove]`).
		Fields(
			service.NewStringField(oFieldBotToken).
				Description("The Slack Bot User OAuth token to use.").
				LintRule(`
        root = if !this.has_prefix("xoxb-") { [ "field must start with xoxb-" ] }
      `),
			service.NewInterpolatedStringField(oFieldChannelID).
				Description("The channel ID containing the message to react to."),
			service.NewInterpolatedStringField(orFieldTimestamp).
				Description("The timestamp of the message to react to."),
			service.NewInterpolatedStringField(orFieldEmoji).
				Description("The name of the emoji to react with (without colons)."),
			service.NewStringEnumField(orFieldAction, "add", "remove").
				Description("Whether to add or remove the reaction.").
				Default("add"),
			service.NewOutputMaxInFlightField(),
		)
}

func newReaction(conf *service.ParsedConfig, _ *service.Resources) (service.Output, int, error) {
	botToken, err := conf.FieldString(oFieldBotToken)
	if err != nil {
		return nil, 0, err
	}
	channelID, err := conf.FieldInterpolatedString(oFieldChannelID)
	if err != nil {
		return nil, 0, err
	}
	timestamp, err := conf.FieldInterpolatedString(orFieldTimestamp)
	if err != nil {
		return nil, 0, err
	}
	emoji, err := conf.FieldInterpolatedString(orFieldEmoji)
	if err != nil {
		return nil, 0, err
	}
	var add bool
	action, err := conf.FieldString(orFieldAction)
	if err != nil {
		return nil, 0, err
	}
	switch action {
	case "add":
		add = true
	case "remove":
		add = false
	default:
		return nil, 0, fmt.Errorf("invalid action '%s', must be 'add' or 'remove'", action)
	}
	maxInFlight, err := conf.FieldMaxInFlight()
	if err != nil {
		return nil, 0, err
	}

	return &reactionOutput{
		api:       slack.New(botToken),
		channelID: channelID,
		timestamp: timestamp,
		emoji:     emoji,
		add:       add,
	}, maxInFlight, nil
}

type reactionOutput struct {
	api       *slack.Client
	channelID *service.InterpolatedString
	timestamp *service.InterpolatedString
	emoji     *service.InterpolatedString
	add       bool
}

var _ service.Output = (*reactionOutput)(nil)

// Connect ensures the Slack token is valid.
func (o *reactionOutput) Connect(ctx context.Context) error {
	_, err := o.api.AuthTestContext(ctx)
	return err
}

// Write applies or removes the reaction based on configuration.
func (o *reactionOutput) Write(ctx context.Context, msg *service.Message) error {
	channelID, err := o.channelID.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate channel ID: %w", err)
	}
	timestamp, err := o.timestamp.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate timestamp: %w", err)
	}
	emoji, err := o.emoji.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to interpolate emoji: %w", err)
	}

	item := slack.ItemRef{Channel: channelID, Timestamp: timestamp}
	if o.add {
		return o.api.AddReactionContext(ctx, emoji, item)
	}
	return o.api.RemoveReactionContext(ctx, emoji, item)
}

// Close is a no-op.
func (*reactionOutput) Close(context.Context) error {
	return nil
}
