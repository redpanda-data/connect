package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newRedisHashOutput), docs.ComponentSpec{
		Name:    "redis_hash",
		Summary: `Sets Redis hash objects using the HMSET command.`,
		Description: output.Description(true, false, `
The field `+"`key`"+` supports
[interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing
you to create a unique key for each message.

The field `+"`fields`"+` allows you to specify an explicit map of field
names to interpolated values, also evaluated per message of a batch:

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

If the field `+"`walk_metadata`"+` is set to `+"`true`"+` then Benthos
will walk all metadata fields of messages and add them to the list of hash
fields to set.

If the field `+"`walk_json_object`"+` is set to `+"`true`"+` then
Benthos will walk each message as a JSON object, extracting keys and the string
representation of their value and adds them to the list of hash fields to set.

The order of hash field extraction is as follows:

1. Metadata (if enabled)
2. JSON object (if enabled)
3. Explicit fields

Where latter stages will overwrite matching field names of a former stage.`),
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString(
				"key", "The key for each message, function interpolations should be used to create a unique key per message.",
				"${!meta(\"kafka_key\")}", "${!json(\"doc.id\")}", "${!count(\"msgs\")}",
			).IsInterpolated(),
			docs.FieldBool("walk_metadata", "Whether all metadata fields of messages should be walked and added to the list of hash fields to set."),
			docs.FieldBool("walk_json_object", "Whether to walk each message as a JSON object and add each key/value pair to the list of hash fields to set."),
			docs.FieldString("fields", "A map of key/value pairs to set as hash fields.").IsInterpolated().Map(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).ChildDefaultAndTypesFromStruct(output.NewRedisHashConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisHashOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	rhash, err := newRedisHashWriter(conf.RedisHash, mgr)
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("redis_hash", conf.RedisHash.MaxInFlight, rhash, mgr)
	if err != nil {
		return nil, err
	}
	return output.OnlySinglePayloads(a), nil
}

type redisHashWriter struct {
	mgr bundle.NewManagement
	log log.Modular

	conf output.RedisHashConfig

	keyStr *field.Expression
	fields map[string]*field.Expression

	client  redis.UniversalClient
	connMut sync.RWMutex
}

func newRedisHashWriter(conf output.RedisHashConfig, mgr bundle.NewManagement) (*redisHashWriter, error) {
	r := &redisHashWriter{
		mgr:    mgr,
		log:    mgr.Logger(),
		conf:   conf,
		fields: map[string]*field.Expression{},
	}

	var err error
	if r.keyStr, err = mgr.BloblEnvironment().NewField(conf.Key); err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	for k, v := range conf.Fields {
		if r.fields[k], err = mgr.BloblEnvironment().NewField(v); err != nil {
			return nil, fmt.Errorf("failed to parse field '%v' expression: %v", k, err)
		}
	}

	if !conf.WalkMetadata && !conf.WalkJSONObject && len(conf.Fields) == 0 {
		return nil, errors.New("at least one mechanism for setting fields must be enabled")
	}

	if _, err := clientFromConfig(mgr.FS(), conf.Config); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *redisHashWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := clientFromConfig(r.mgr.FS(), r.conf.Config)
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}

	r.log.Infoln("Setting messages as hash objects to Redis")

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

func walkForHashFields(
	msg message.Batch, index int, fields map[string]any,
) error {
	jVal, err := msg.Get(index).AsStructured()
	if err != nil {
		return err
	}
	jObj, ok := jVal.(map[string]any)
	if !ok {
		return fmt.Errorf("expected JSON object, found '%T'", jVal)
	}
	for k, v := range jObj {
		fields[k] = v
	}
	return nil
}

func (r *redisHashWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		key, err := r.keyStr.String(i, msg)
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}
		fields := map[string]any{}
		if r.conf.WalkMetadata {
			_ = p.MetaIterMut(func(k string, v any) error {
				fields[k] = v
				return nil
			})
		}
		if r.conf.WalkJSONObject {
			if err := walkForHashFields(msg, i, fields); err != nil {
				err = fmt.Errorf("failed to walk JSON object: %v", err)
				r.log.Errorf("HMSET error: %v\n", err)
				return err
			}
		}
		for k, v := range r.fields {
			if fields[k], err = v.String(i, msg); err != nil {
				return fmt.Errorf("field %v interpolation error: %w", k, err)
			}
		}
		if err := client.HMSet(ctx, key, fields).Err(); err != nil {
			_ = r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return component.ErrNotConnected
		}
		return nil
	})
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
