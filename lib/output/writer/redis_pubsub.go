package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	ibatch "github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	bredis "github.com/Jeffail/benthos/v3/internal/impl/redis"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisPubSubConfig contains configuration fields for the RedisPubSub output
// type.
type RedisPubSubConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Channel       string             `json:"channel" yaml:"channel"`
	MaxInFlight   int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching      batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewRedisPubSubConfig creates a new RedisPubSubConfig with default values.
func NewRedisPubSubConfig() RedisPubSubConfig {
	return RedisPubSubConfig{
		Config:      bredis.NewConfig(),
		Channel:     "benthos_chan",
		MaxInFlight: 1,
		Batching:    batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// RedisPubSub is an output type that serves RedisPubSub messages.
type RedisPubSub struct {
	log   log.Modular
	stats metrics.Type

	conf       RedisPubSubConfig
	channelStr *field.Expression

	client  redis.UniversalClient
	connMut sync.RWMutex
}

// NewRedisPubSubV2 creates a new RedisPubSub output type.
func NewRedisPubSubV2(
	conf RedisPubSubConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*RedisPubSub, error) {
	r := &RedisPubSub{
		log:   log,
		stats: stats,
		conf:  conf,
	}
	var err error
	if r.channelStr, err = interop.NewBloblangField(mgr, conf.Channel); err != nil {
		return nil, fmt.Errorf("failed to parse channel expression: %v", err)
	}
	if _, err = conf.Config.Client(); err != nil {
		return nil, err
	}
	return r, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an RedisPubSub server.
func (r *RedisPubSub) ConnectWithContext(ctx context.Context) error {
	return r.Connect()
}

// Connect establishes a connection to an RedisPubSub server.
func (r *RedisPubSub) Connect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.conf.Config.Client()
	if err != nil {
		return err
	}
	if _, err = client.Ping().Result(); err != nil {
		return err
	}

	r.log.Infof("Pushing messages to Redis channel: %v\n", r.conf.Channel)

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

// WriteWithContext attempts to write a message by pushing it to a Redis pub/sub
// topic.
func (r *RedisPubSub) WriteWithContext(ctx context.Context, msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	if msg.Len() == 1 {
		channel := r.channelStr.String(0, msg)
		if err := client.Publish(channel, msg.Get(0).Get()).Err(); err != nil {
			r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return types.ErrNotConnected
		}
		return nil
	}

	pipe := client.Pipeline()
	msg.Iter(func(i int, p types.Part) error {
		_ = pipe.Publish(r.channelStr.String(i, msg), p.Get())
		return nil
	})
	cmders, err := pipe.Exec()
	if err != nil {
		r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return types.ErrNotConnected
	}

	var batchErr *ibatch.Error
	for i, res := range cmders {
		if res.Err() != nil {
			if batchErr == nil {
				batchErr = ibatch.NewError(msg, res.Err())
			}
			batchErr.Failed(i, res.Err())
		}
	}
	if batchErr != nil {
		return batchErr
	}
	return nil
}

// Write attempts to write a message by pushing it to a Redis pub/sub topic.
func (r *RedisPubSub) Write(msg types.Message) error {
	return r.WriteWithContext(context.Background(), msg)
}

// disconnect safely closes a connection to an RedisPubSub server.
func (r *RedisPubSub) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

// CloseAsync shuts down the RedisPubSub output and stops processing messages.
func (r *RedisPubSub) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisPubSub output has closed down.
func (r *RedisPubSub) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
