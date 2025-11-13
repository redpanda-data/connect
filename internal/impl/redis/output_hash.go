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
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	hoFieldKey          = "key"
	hoFieldWalkMetadata = "walk_metadata"
	hoFieldWalkJSON     = "walk_json_object"
	hoFieldFields       = "fields"
	hoFieldBatching     = "batching"
)

func redisHashOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Sets Redis hash objects using the HSET command.`).
		Description(`
The field `+"`key`"+` supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions], allowing you to create a unique key for each message.

The field `+"`fields`"+` allows you to specify an explicit map of field names to interpolated values, also evaluated per message of a batch:

`+"```yaml"+`
output:
  redis_hash:
    url: tcp://localhost:6379
    key: ${!json("id")}
    fields:
      topic: ${!meta("kafka_topic")}
      partition: ${!meta("kafka_partition")}
      content: ${!json("document.text")}
`+"```"+`

If the field `+"`walk_metadata`"+` is set to `+"`true`"+` then Redpanda Connect will walk all metadata fields of messages and add them to the list of hash fields to set.

If the field `+"`walk_json_object`"+` is set to `+"`true`"+` then Redpanda Connect will walk each message as a JSON object, extracting keys and the string representation of their value and adds them to the list of hash fields to set.

The order of hash field extraction is as follows:

1. Metadata (if enabled)
2. JSON object (if enabled)
3. Explicit fields

Where latter stages will overwrite matching field names of a former stage.`+service.OutputPerformanceDocs(true, false)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(hoFieldKey).
				Description("The key for each message, function interpolations should be used to create a unique key per message.").
				Examples("${! @.kafka_key }", "${! this.doc.id }", "${! counter() }"),
			service.NewBoolField(hoFieldWalkMetadata).
				Description("Whether all metadata fields of messages should be walked and added to the list of hash fields to set.").
				Default(false),
			service.NewBoolField(hoFieldWalkJSON).
				Description("Whether to walk each message as a JSON object and add each key/value pair to the list of hash fields to set.").
				Default(false),
			service.NewInterpolatedStringMapField(hoFieldFields).
				Description("A map of key/value pairs to set as hash fields.").
				Default(map[string]any{}),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(loFieldBatching),
		)
}

func init() {
	service.MustRegisterBatchOutput(
		"redis_hash", redisHashOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, maxInFlight int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(loFieldBatching); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newRedisHashWriter(conf, mgr)
			return
		})
}

type redisHashWriter struct {
	log *service.Logger

	key          *service.InterpolatedString
	walkMetadata bool
	walkJSON     bool
	fields       map[string]*service.InterpolatedString

	clientCtor func() (redis.UniversalClient, error)
	client     redis.UniversalClient
	connMut    sync.RWMutex
}

func newRedisHashWriter(conf *service.ParsedConfig, mgr *service.Resources) (r *redisHashWriter, err error) {
	r = &redisHashWriter{
		clientCtor: func() (redis.UniversalClient, error) {
			return getClient(conf)
		},
		log: mgr.Logger(),
	}
	if _, err = getClient(conf); err != nil {
		return
	}

	if r.key, err = conf.FieldInterpolatedString(hoFieldKey); err != nil {
		return
	}
	if r.walkMetadata, err = conf.FieldBool(hoFieldWalkMetadata); err != nil {
		return
	}
	if r.walkJSON, err = conf.FieldBool(hoFieldWalkJSON); err != nil {
		return
	}
	if r.fields, err = conf.FieldInterpolatedStringMap(hoFieldFields); err != nil {
		return
	}

	if !r.walkMetadata && !r.walkJSON && len(r.fields) == 0 {
		return nil, errors.New("at least one mechanism for setting fields must be enabled")
	}
	return
}

func (r *redisHashWriter) Connect(ctx context.Context) error {
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

//------------------------------------------------------------------------------

func walkForHashFields(msg *service.Message, fields map[string]any) error {
	jVal, err := msg.AsStructured()
	if err != nil {
		return err
	}
	jObj, ok := jVal.(map[string]any)
	if !ok {
		return fmt.Errorf("expected JSON object, found '%T'", jVal)
	}
	maps.Copy(fields, jObj)
	return nil
}

func (r *redisHashWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	if len(batch) == 1 {
		key, err := r.key.TryString(batch[0])
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}
		fields := map[string]any{}
		if r.walkMetadata {
			_ = batch[0].MetaWalkMut(func(k string, v any) error {
				fields[k] = v
				return nil
			})
		}
		if r.walkJSON {
			if err := walkForHashFields(batch[0], fields); err != nil {
				err = fmt.Errorf("failed to walk JSON object: %v", err)
				r.log.Errorf("HSET error: %v\n", err)
				return err
			}
		}
		for k, v := range r.fields {
			if fields[k], err = v.TryString(batch[0]); err != nil {
				return fmt.Errorf("field %v interpolation error: %w", k, err)
			}
		}
		if err := client.HSet(ctx, key, fields).Err(); err != nil {
			_ = r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return service.ErrNotConnected
		}
		return nil
	}

	pipe := client.Pipeline()

	for i := range batch {
		key, err := batch.TryInterpolatedString(i, r.key)
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}

		fields := map[string]any{}
		if r.walkMetadata {
			_ = batch[i].MetaWalkMut(func(k string, v any) error {
				fields[k] = v
				return nil
			})
		}
		if r.walkJSON {
			if err := walkForHashFields(batch[i], fields); err != nil {
				err = fmt.Errorf("failed to walk JSON object: %v", err)
				r.log.Errorf("HSET error: %v\n", err)
				return err
			}
		}
		for k, v := range r.fields {
			if fields[k], err = v.TryString(batch[i]); err != nil {
				return fmt.Errorf("field %v interpolation error: %w", k, err)
			}
		}
		_ = pipe.HSet(ctx, key, fields)
	}

	cmders, err := pipe.Exec(ctx)
	if err != nil {
		_ = r.disconnect()
		r.log.Errorf("Errorf from redis: %v\n", err)
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

func (r *redisHashWriter) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

func (r *redisHashWriter) Close(context.Context) error {
	return r.disconnect()
}
