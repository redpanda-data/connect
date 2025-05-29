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

package redis

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	psiFieldChannels    = "channels"
	psiFieldUsePatterns = "use_patterns"
)

func redisPubSubInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Consume from a Redis publish/subscribe channel using either the SUBSCRIBE or PSUBSCRIBE commands.`).
		Description(`
In order to subscribe to channels using the `+"`PSUBSCRIBE`"+` command set the field `+"`use_patterns` to `true`"+`, then you can include glob-style patterns in your channel names. For example:

- `+"`h?llo`"+` subscribes to hello, hallo and hxllo
- `+"`h*llo`"+` subscribes to hllo and heeeello
- `+"`h[ae]llo`"+` subscribes to hello and hallo, but not hillo

Use `+"`\\`"+` to escape special characters if you want to match them verbatim.`).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewStringListField(psiFieldChannels).
				Description("A list of channels to consume from."),
			service.NewBoolField(psiFieldUsePatterns).
				Description("Whether to use the PSUBSCRIBE command, allowing for glob-style patterns within target channel names.").
				Default(false),
			service.NewAutoRetryNacksToggleField(),
		)
}

func init() {
	service.MustRegisterInput(
		"redis_pubsub", redisPubSubInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			r, err := newRedisPubSubReader(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, r)
		})
}

type redisPubSubReader struct {
	client redis.UniversalClient
	pubsub *redis.PubSub
	cMut   sync.Mutex

	channels    []string
	usePatterns bool

	log *service.Logger
}

func newRedisPubSubReader(conf *service.ParsedConfig, mgr *service.Resources) (*redisPubSubReader, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}
	r := &redisPubSubReader{
		client: client,
		log:    mgr.Logger(),
	}
	if r.channels, err = conf.FieldStringList(psiFieldChannels); err != nil {
		return nil, err
	}
	if r.usePatterns, err = conf.FieldBool(psiFieldUsePatterns); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redisPubSubReader) Connect(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.pubsub != nil {
		return nil
	}

	if _, err := r.client.Ping(ctx).Result(); err != nil {
		return err
	}

	if r.usePatterns {
		r.pubsub = r.client.PSubscribe(ctx, r.channels...)
	} else {
		r.pubsub = r.client.Subscribe(ctx, r.channels...)
	}
	return nil
}

func (r *redisPubSubReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	var pubsub *redis.PubSub

	r.cMut.Lock()
	pubsub = r.pubsub
	r.cMut.Unlock()

	if pubsub == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case rMsg, open := <-pubsub.Channel():
		if !open {
			_ = r.disconnect()
			return nil, nil, service.ErrEndOfInput
		}
		return service.NewMessage([]byte(rMsg.Payload)), func(context.Context, error) error {
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (r *redisPubSubReader) disconnect() error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.pubsub != nil {
		err = r.pubsub.Close()
		r.pubsub = nil
	}
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

func (r *redisPubSubReader) Close(context.Context) (err error) {
	err = r.disconnect()
	return
}
