// Copyright 2024 Redpanda Data, Inc.
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

package discord

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/bwmarrin/discordgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func outputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services", "Social").
		Summary("Writes messages to a Discord channel.").
		Description(`
This output POSTs messages to the `+"`/channels/\\{channel_id}/messages`"+` Discord API endpoint authenticated as a bot using token based authentication.

If the format of a message is a JSON object matching the https://discord.com/developers/docs/resources/channel#message-object[Discord API message type^] then it is sent directly, otherwise an object matching the API type is created with the content of the message added as a string.
`).
		Fields(
			service.NewStringField("channel_id").
				Description("A discord channel ID to write messages to."),
			service.NewStringField("bot_token").
				Description("A bot token used for authentication."),

			// Deprecated
			service.NewStringField("rate_limit").
				Description("").
				Default("An optional rate limit resource to restrict API requests with.").
				Deprecated(),
		)
}

func init() {
	service.MustRegisterOutput(
		"discord", outputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newWriter(conf, mgr)
			return w, 1, err
		},
	)
}

type writer struct {
	mgr *service.Resources
	log *service.Logger

	// Config
	channelID string
	botToken  string

	connMut sync.Mutex
	sess    *discordgo.Session
	done    func()
}

func newWriter(conf *service.ParsedConfig, mgr *service.Resources) (*writer, error) {
	w := &writer{
		mgr: mgr,
		log: mgr.Logger(),
	}
	var err error
	if w.channelID, err = conf.FieldString("channel_id"); err != nil {
		return nil, err
	}
	if w.botToken, err = conf.FieldString("bot_token"); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *writer) Connect(context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()
	if w.sess != nil {
		return nil
	}

	var err error
	if w.sess, w.done, err = getGlobalSession(w.botToken, w.mgr.EngineVersion()); err != nil {
		return err
	}
	return nil
}

func (w *writer) Write(ctx context.Context, msg *service.Message) error {
	w.connMut.Lock()
	sess := w.sess
	w.connMut.Unlock()
	if sess == nil {
		return service.ErrNotConnected
	}

	rawContent, err := msg.AsBytes()
	if err != nil {
		return err
	}

	var cMsg discordgo.MessageSend
	if err := json.Unmarshal(rawContent, &cMsg); err == nil {
		_, err = sess.ChannelMessageSendComplex(w.channelID, &cMsg)
		return err
	}

	_, err = sess.ChannelMessageSend(w.channelID, string(rawContent), []discordgo.RequestOption{discordgo.WithContext(ctx)}...)
	return err
}

func (w *writer) Close(context.Context) error {
	w.connMut.Lock()
	if w.done != nil {
		w.done()
		w.sess = nil
	}
	w.connMut.Unlock()
	return nil
}
