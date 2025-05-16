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
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/slack-go/slack"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	service.MustRegisterInput("slack_users", usersInputSpec(), newUsersInput)
}

const (
	iFieldTeamID = "team_id"
)

func usersInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Description(`Reads all users in a slack organization (optionally filtered by a team ID).`).
		Fields(
			service.NewStringField(iFieldBotToken).Description("The Slack Bot User OAuth token to use.").LintRule(`
        root = if !this.has_prefix("xoxb-") { [ "field must start with xoxb-" ] }
      `),
			service.NewStringField(iFieldTeamID).Description("The team ID to filter by").Default(""),
			service.NewAutoRetryNacksToggleField(),
		)
}

func newUsersInput(conf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
	botToken, err := conf.FieldString(iFieldBotToken)
	if err != nil {
		return nil, err
	}
	teamID, err := conf.FieldString(iFieldTeamID)
	if err != nil {
		return nil, err
	}
	var opts []slack.GetUsersOption
	if teamID != "" {
		opts = append(opts, slack.GetUsersOptionTeamID(teamID))
	}
	return service.AutoRetryNacksToggled(conf, &usersInput{
		botToken: botToken,
		opts:     opts,
		channel:  make(chan readResult),
		log:      res.Logger(),
	})
}

type readResult struct {
	user json.RawMessage
	err  error
}

type usersInput struct {
	botToken string
	opts     []slack.GetUsersOption

	log     *service.Logger
	shutSig *shutdown.Signaller
	channel chan readResult
}

func (i *usersInput) Connect(ctx context.Context) error {
	if i.shutSig != nil {
		select {
		case <-i.shutSig.HasStoppedChan():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	api := slack.New(i.botToken)
	shutSig := shutdown.NewSignaller()
	go func() {
		defer shutSig.TriggerHasStopped()
		ctx, cancel := shutSig.HardStopCtx(context.Background())
		defer cancel()
		var err error
		p := api.GetUsersPaginated(i.opts...)
		for err == nil {
			p, err = p.Next(ctx)
			if err == nil {
				for _, user := range p.Users {
					var b []byte
					b, err = json.Marshal(user)
					select {
					case i.channel <- readResult{user: b}:
					case <-ctx.Done():
						err = ctx.Err()
					}
					if err != nil {
						break
					}
				}
			} else if rateLimitedError, ok := err.(*slack.RateLimitedError); ok {
				select {
				case <-ctx.Done():
					err = ctx.Err()
				case <-time.After(rateLimitedError.RetryAfter):
					err = nil
				}
			}
		}
		err = p.Failure(err)
		if err != nil {
			i.channel <- readResult{err: err}
		}
	}()
	i.shutSig = shutSig
	return nil
}

func (i *usersInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for {
		select {
		case result := <-i.channel:
			if result.err != nil {
				return nil, nil, result.err
			}
			return service.NewMessage(result.user), func(context.Context, error) error { return nil }, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-i.shutSig.HasStoppedChan():
			return nil, nil, service.ErrEndOfInput
		}
	}
}

func (i *usersInput) Close(ctx context.Context) error {
	if i.shutSig == nil {
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
