package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/output"
	bredis "github.com/Jeffail/benthos/v3/internal/service/redis"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisStreamsConfig contains configuration fields for the RedisStreams output type.
type RedisStreamsConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Stream        string          `json:"stream" yaml:"stream"`
	BodyKey       string          `json:"body_key" yaml:"body_key"`
	MaxLenApprox  int64           `json:"max_length" yaml:"max_length"`
	MaxInFlight   int             `json:"max_in_flight" yaml:"max_in_flight"`
	Metadata      output.Metadata `json:"metadata" yaml:"metadata"`
}

// NewRedisStreamsConfig creates a new RedisStreamsConfig with default values.
func NewRedisStreamsConfig() RedisStreamsConfig {
	return RedisStreamsConfig{
		Config:       bredis.NewConfig(),
		Stream:       "benthos_stream",
		BodyKey:      "body",
		MaxLenApprox: 0,
		MaxInFlight:  1,
		Metadata:     output.NewMetadata(),
	}
}

//------------------------------------------------------------------------------

// RedisStreams is an output type that serves RedisStreams messages.
type RedisStreams struct {
	log   log.Modular
	stats metrics.Type

	conf       RedisStreamsConfig
	metaFilter *output.MetadataFilter

	client  redis.UniversalClient
	connMut sync.RWMutex
}

// NewRedisStreams creates a new RedisStreams output type.
func NewRedisStreams(
	conf RedisStreamsConfig,
	log log.Modular,
	stats metrics.Type,
) (*RedisStreams, error) {

	r := &RedisStreams{
		log:   log,
		stats: stats,
		conf:  conf,
	}

	var err error
	if r.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	if _, err = conf.Config.Client(); err != nil {
		return nil, err
	}
	return r, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an RedisStreams server.
func (r *RedisStreams) ConnectWithContext(ctx context.Context) error {
	return r.Connect()
}

// Connect establishes a connection to an RedisStreams server.
func (r *RedisStreams) Connect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.conf.Config.Client()
	if err != nil {
		return err
	}
	if _, err = client.Ping().Result(); err != nil {
		return err
	}

	r.log.Infof("Pushing messages to Redis stream: %v\n", r.conf.Stream)

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

// WriteWithContext attempts to write a message by pushing it to a Redis stream.
func (r *RedisStreams) WriteWithContext(ctx context.Context, msg types.Message) error {
	return r.Write(msg)
}

// Write attempts to write a message by pushing it to a Redis stream.
func (r *RedisStreams) Write(msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		values := map[string]interface{}{}
		r.metaFilter.Iter(p.Metadata(), func(k, v string) error {
			values[k] = v
			return nil
		})
		values[r.conf.BodyKey] = p.Get()
		if err := client.XAdd(&redis.XAddArgs{
			ID:           "*",
			Stream:       r.conf.Stream,
			MaxLenApprox: r.conf.MaxLenApprox,
			Values:       values,
		}).Err(); err != nil {
			r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return types.ErrNotConnected
		}
		return nil
	})
}

// disconnect safely closes a connection to an RedisStreams server.
func (r *RedisStreams) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

// CloseAsync shuts down the RedisStreams output and stops processing messages.
func (r *RedisStreams) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisStreams output has closed down.
func (r *RedisStreams) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
