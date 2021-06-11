package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	bredis "github.com/Jeffail/benthos/v3/internal/impl/redis"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisListConfig contains configuration fields for the RedisList output type.
type RedisListConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Key           string `json:"key" yaml:"key"`
	MaxInFlight   int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewRedisListConfig creates a new RedisListConfig with default values.
func NewRedisListConfig() RedisListConfig {
	return RedisListConfig{
		Config:      bredis.NewConfig(),
		Key:         "benthos_list",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// RedisList is an output type that serves RedisList messages.
type RedisList struct {
	log   log.Modular
	stats metrics.Type

	conf RedisListConfig

	keyStr *field.Expression

	client  redis.UniversalClient
	connMut sync.RWMutex
}

// NewRedisList creates a new RedisList output type.
func NewRedisList(
	conf RedisListConfig,
	log log.Modular,
	stats metrics.Type,
) (*RedisList, error) {
	r := &RedisList{
		log:   log,
		stats: stats,
		conf:  conf,
	}

	var err error
	if r.keyStr, err = bloblang.NewField(conf.Key); err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}
	if _, err := conf.Config.Client(); err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an RedisList server.
func (r *RedisList) ConnectWithContext(ctx context.Context) error {
	return r.Connect()
}

// Connect establishes a connection to an RedisList server.
func (r *RedisList) Connect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.conf.Config.Client()
	if err != nil {
		return err
	}
	if _, err = client.Ping().Result(); err != nil {
		return err
	}

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

// WriteWithContext attempts to write a message by pushing it to the end of a
// Redis list.
func (r *RedisList) WriteWithContext(ctx context.Context, msg types.Message) error {
	return r.Write(msg)
}

// Write attempts to write a message by pushing it to the end of a Redis list.
func (r *RedisList) Write(msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		key := r.keyStr.String(i, msg)
		if err := client.RPush(key, p.Get()).Err(); err != nil {
			r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return types.ErrNotConnected
		}
		return nil
	})
}

// disconnect safely closes a connection to an RedisList server.
func (r *RedisList) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

// CloseAsync shuts down the RedisList output and stops processing messages.
func (r *RedisList) CloseAsync() {
	go r.disconnect()
}

// WaitForClose blocks until the RedisList output has closed down.
func (r *RedisList) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
