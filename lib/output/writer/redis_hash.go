package writer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	bredis "github.com/Jeffail/benthos/v3/internal/impl/redis"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisHashConfig contains configuration fields for the RedisHash output type.
type RedisHashConfig struct {
	bredis.Config  `json:",inline" yaml:",inline"`
	Key            string            `json:"key" yaml:"key"`
	WalkMetadata   bool              `json:"walk_metadata" yaml:"walk_metadata"`
	WalkJSONObject bool              `json:"walk_json_object" yaml:"walk_json_object"`
	Fields         map[string]string `json:"fields" yaml:"fields"`
	MaxInFlight    int               `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewRedisHashConfig creates a new RedisHashConfig with default values.
func NewRedisHashConfig() RedisHashConfig {
	return RedisHashConfig{
		Config:         bredis.NewConfig(),
		Key:            "",
		WalkMetadata:   false,
		WalkJSONObject: false,
		Fields:         map[string]string{},
		MaxInFlight:    1,
	}
}

//------------------------------------------------------------------------------

// RedisHash is an output type that writes hash objects to Redis using the HMSET
// command.
type RedisHash struct {
	log   log.Modular
	stats metrics.Type

	conf RedisHashConfig

	keyStr *field.Expression
	fields map[string]*field.Expression

	client  redis.UniversalClient
	connMut sync.RWMutex
}

// NewRedisHashV2 creates a new RedisHash output type.
func NewRedisHashV2(
	conf RedisHashConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*RedisHash, error) {
	r := &RedisHash{
		log:    log,
		stats:  stats,
		conf:   conf,
		fields: map[string]*field.Expression{},
	}

	var err error
	if r.keyStr, err = interop.NewBloblangField(mgr, conf.Key); err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	for k, v := range conf.Fields {
		if r.fields[k], err = interop.NewBloblangField(mgr, v); err != nil {
			return nil, fmt.Errorf("failed to parse field '%v' expression: %v", k, err)
		}
	}

	if !conf.WalkMetadata && !conf.WalkJSONObject && len(conf.Fields) == 0 {
		return nil, errors.New("at least one mechanism for setting fields must be enabled")
	}

	if _, err := conf.Config.Client(); err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an RedisHash server.
func (r *RedisHash) ConnectWithContext(ctx context.Context) error {
	return r.Connect()
}

// Connect establishes a connection to an RedisHash server.
func (r *RedisHash) Connect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.conf.Config.Client()
	if err != nil {
		return err
	}
	if _, err = client.Ping().Result(); err != nil {
		return err
	}

	r.log.Infoln("Setting messages as hash objects to Redis")

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

func walkForHashFields(
	msg types.Message, index int, fields map[string]interface{},
) error {
	jVal, err := msg.Get(index).JSON()
	if err != nil {
		return err
	}
	jObj, ok := jVal.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected JSON object, found '%T'", jVal)
	}
	for k, v := range jObj {
		fields[k] = v
	}
	return nil
}

// WriteWithContext attempts to write a message to Redis by setting it using the
// HMSET command.
func (r *RedisHash) WriteWithContext(ctx context.Context, msg types.Message) error {
	return r.Write(msg)
}

// Write attempts to write a message to Redis by setting it using the HMSET
// command.
func (r *RedisHash) Write(msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		key := r.keyStr.String(i, msg)
		fields := map[string]interface{}{}
		if r.conf.WalkMetadata {
			p.Metadata().Iter(func(k, v string) error {
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
			fields[k] = v.String(i, msg)
		}
		if err := client.HMSet(key, fields).Err(); err != nil {
			r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return types.ErrNotConnected
		}
		return nil
	})
}

// disconnect safely closes a connection to an RedisHash server.
func (r *RedisHash) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

// CloseAsync shuts down the RedisHash output and stops processing messages.
func (r *RedisHash) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisHash output has closed down.
func (r *RedisHash) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
