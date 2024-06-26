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
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	psoFieldChannel  = "channel"
	psoFieldBatching = "batching"
)

func redisPubSubOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Publishes messages through the Redis PubSub model. It is not possible to guarantee that messages have been received.`).
		Description(`
This output will interpolate functions within the channel field, you can find a list of functions xref:configuration:interpolation.adoc#bloblang-queries[here].`+service.OutputPerformanceDocs(true, true)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(psoFieldChannel).
				Description("The channel to publish messages to."),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(psoFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"redis_pubsub", redisPubSubOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(psoFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newRedisPubSubWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type redisPubSubWriter struct {
	log *service.Logger

	channelStr string
	channel    *service.InterpolatedString

	clientCtor func() (redis.UniversalClient, error)
	client     redis.UniversalClient
	connMut    sync.RWMutex
}

func newRedisPubSubWriter(conf *service.ParsedConfig, mgr *service.Resources) (r *redisPubSubWriter, err error) {
	r = &redisPubSubWriter{
		log: mgr.Logger(),
		clientCtor: func() (redis.UniversalClient, error) {
			return getClient(conf)
		},
	}

	if r.channelStr, err = conf.FieldString(psoFieldChannel); err != nil {
		return
	}
	if r.channel, err = conf.FieldInterpolatedString(psoFieldChannel); err != nil {
		return
	}

	if _, err := getClient(conf); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redisPubSubWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.clientCtor()
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}
	r.client = client
	return nil
}

func (r *redisPubSubWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	if len(batch) == 1 {
		channel, err := r.channel.TryString(batch[0])
		if err != nil {
			return fmt.Errorf("channel interpolation error: %w", err)
		}

		mBytes, err := batch[0].AsBytes()
		if err != nil {
			return err
		}

		if err := client.Publish(ctx, channel, mBytes).Err(); err != nil {
			_ = r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return service.ErrNotConnected
		}
		return nil
	}

	pipe := client.Pipeline()

	for i := 0; i < len(batch); i++ {
		channel, err := batch.TryInterpolatedString(i, r.channel)
		if err != nil {
			return fmt.Errorf("channel interpolation error: %w", err)
		}

		mBytes, err := batch[i].AsBytes()
		if err != nil {
			return err
		}

		_ = pipe.Publish(ctx, channel, mBytes)
	}

	cmders, err := pipe.Exec(ctx)
	if err != nil {
		_ = r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return service.ErrNotConnected
	}

	var batchErr *service.BatchError
	for i, res := range cmders {
		if res.Err() != nil {
			if batchErr == nil {
				batchErr = service.NewBatchError(batch, res.Err())
			}
			batchErr.Failed(i, res.Err())
		}
	}
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (r *redisPubSubWriter) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

func (r *redisPubSubWriter) Close(context.Context) error {
	return r.disconnect()
}
