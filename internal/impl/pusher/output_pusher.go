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

package pusher

import (
	"context"

	"github.com/pusher/pusher-http-go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func pusherOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.3.0").
		Summary("Output for publishing messages to Pusher API (https://pusher.com)").
		Field(service.NewBatchPolicyField("batching").
			Description("maximum batch size is 10 (limit of the pusher library)")).
		Field(service.NewInterpolatedStringField("channel").
			Description("Pusher channel to publish to. Interpolation functions can also be used").
			Example("my_channel").
			Example("${!json(\"id\")}")).
		Field(service.NewStringField("event").
			Description("Event to publish to")).
		Field(service.NewStringField("appId").
			Description("Pusher app id")).
		Field(service.NewStringField("key").
			Description("Pusher key")).
		Field(service.NewStringField("secret").
			Description("Pusher secret")).
		Field(service.NewStringField("cluster").
			Description("Pusher cluster")).
		Field(service.NewBoolField("secure").
			Description("Enable SSL encryption").
			Default(true)).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of parallel message batches to have in flight at any given time.").
			Default(1))
}

func init() {
	service.MustRegisterBatchOutput("pusher", pusherOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newPusherWriterFromConfig(conf, mgr.Logger())
			return
		})
}

type pusherWriter struct {
	log *service.Logger

	event   string
	appID   string
	key     string
	secret  string
	cluster string
	secure  bool
	channel *service.InterpolatedString

	client pusher.Client
}

func newPusherWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*pusherWriter, error) {
	p := pusherWriter{
		log: log,
	}

	var err error

	// check and write all variables to config

	if p.channel, err = conf.FieldInterpolatedString("channel"); err != nil {
		return nil, err
	}

	if p.event, err = conf.FieldString("event"); err != nil {
		return nil, err
	}
	if p.appID, err = conf.FieldString("appId"); err != nil {
		return nil, err
	}
	if p.key, err = conf.FieldString("key"); err != nil {
		return nil, err
	}
	if p.secret, err = conf.FieldString("secret"); err != nil {
		return nil, err
	}
	if p.cluster, err = conf.FieldString("cluster"); err != nil {
		return nil, err
	}
	if p.secure, err = conf.FieldBool("secure"); err != nil {
		return nil, err
	}

	return &p, nil
}

func (p *pusherWriter) Connect(context.Context) error {
	// create pusher client
	p.client = pusher.Client{
		AppID:   p.appID,
		Key:     p.key,
		Secret:  p.secret,
		Cluster: p.cluster,
		Secure:  p.secure,
	}
	return nil
}

func (p *pusherWriter) WriteBatch(_ context.Context, b service.MessageBatch) (err error) {
	events := make([]pusher.Event, 0, len(b))

	// iterate over batch and set pusher events in array
	for _, msg := range b {
		content, err := msg.AsBytes()
		if err != nil {
			return err
		}

		key, err := p.channel.TryString(msg)
		if err != nil {
			return err
		}

		event := pusher.Event{
			Channel: key,
			Name:    p.event,
			Data:    content,
		}
		events = append(events, event)
	}
	// send event array to pusher
	err = p.client.TriggerBatch(events)
	return err
}

func (p *pusherWriter) Close(context.Context) error {
	// p.client.HTTPClient might be nil if this output was never used. See: https://github.com/pusher/pusher-http-go/blob/v4.0.1/client.go#L115
	if p.client.HTTPClient != nil {
		p.client.HTTPClient.CloseIdleConnections()
	}
	p.log.Debug("Pusher connection closed")
	return nil
}
