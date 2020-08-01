package writer

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisHashConfig contains configuration fields for the RedisHash output type.
type RedisHashConfig struct {
	URL            string            `json:"url" yaml:"url"`
	Key            string            `json:"key" yaml:"key"`
	WalkMetadata   bool              `json:"walk_metadata" yaml:"walk_metadata"`
	WalkJSONObject bool              `json:"walk_json_object" yaml:"walk_json_object"`
	Fields         map[string]string `json:"fields" yaml:"fields"`
	MaxInFlight    int               `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewRedisHashConfig creates a new RedisHashConfig with default values.
func NewRedisHashConfig() RedisHashConfig {
	return RedisHashConfig{
		URL:            "tcp://localhost:6379",
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

	url  *url.URL
	conf RedisHashConfig

	keyStr field.Expression
	fields map[string]field.Expression

	client  *redis.Client
	connMut sync.RWMutex
}

// NewRedisHash creates a new RedisHash output type.
func NewRedisHash(
	conf RedisHashConfig,
	log log.Modular,
	stats metrics.Type,
) (*RedisHash, error) {
	r := &RedisHash{
		log:    log,
		stats:  stats,
		conf:   conf,
		fields: map[string]field.Expression{},
	}

	var err error
	if r.keyStr, err = field.New(conf.Key); err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	for k, v := range conf.Fields {
		if r.fields[k], err = field.New(v); err != nil {
			return nil, fmt.Errorf("failed to parse field '%v' expression: %v", k, err)
		}
	}

	if !conf.WalkMetadata && !conf.WalkJSONObject && len(conf.Fields) == 0 {
		return nil, errors.New("at least one mechanism for setting fields must be enabled")
	}

	r.url, err = url.Parse(conf.URL)
	if err != nil {
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

	var pass string
	if r.url.User != nil {
		pass, _ = r.url.User.Password()
	}
	client := redis.NewClient(&redis.Options{
		Addr:     r.url.Host,
		Network:  r.url.Scheme,
		Password: pass,
	})

	if _, err := client.Ping().Result(); err != nil {
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
