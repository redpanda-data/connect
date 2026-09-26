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
		Summary("Returns the full profile of all users in your Slack organization using the users.list API method. You can filter the returned users by team ID.").
		Description(`This input reads users with the https://api.slack.com/methods/users.list[`+"`users.list`"+`^] Slack API method and emits each user profile as a separate message. To return only the users of one team, set `+"`team_id`"+`.`).
		Fields(
			service.NewStringField(iFieldBotToken).Description("Your https://api.slack.com/concepts/token-types[Slack bot user's OAuth token^], which must have the https://api.slack.com/scopes/users:read[`users.read` scope^] to access your Slack organization.").LintRule(`
        root = if !this.has_prefix("xoxb-") { [ "field must start with xoxb-" ] }
      `),
			service.NewStringField(iFieldTeamID).Description("The encoded ID of a Slack team by which to filter the list of returned users, which you can get from the https://api.slack.com/methods/team.info[`team.info` Slack API method^]. If `team_id` is left empty, users from all teams within the organization are returned.").Default(""),
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
