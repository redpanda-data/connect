package reader

import (
	"context"
	"sync"
	"time"

	bredis "github.com/Jeffail/benthos/v3/internal/impl/redis/old"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

// RedisPubSubConfig contains configuration fields for the RedisPubSub input
// type.
type RedisPubSubConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Channels      []string `json:"channels" yaml:"channels"`
	UsePatterns   bool     `json:"use_patterns" yaml:"use_patterns"`
}

// NewRedisPubSubConfig creates a new RedisPubSubConfig with default values.
func NewRedisPubSubConfig() RedisPubSubConfig {
	return RedisPubSubConfig{
		Config:      bredis.NewConfig(),
		Channels:    []string{"benthos_chan"},
		UsePatterns: false,
	}
}

//------------------------------------------------------------------------------

// RedisPubSub is an input type that reads Redis Pub/Sub messages.
type RedisPubSub struct {
	client redis.UniversalClient
	pubsub *redis.PubSub
	cMut   sync.Mutex

	conf RedisPubSubConfig

	stats metrics.Type
	log   log.Modular
}

// NewRedisPubSub creates a new RedisPubSub input type.
func NewRedisPubSub(
	conf RedisPubSubConfig, log log.Modular, stats metrics.Type,
) (*RedisPubSub, error) {
	r := &RedisPubSub{
		conf:  conf,
		stats: stats,
		log:   log,
	}

	_, err := r.conf.Config.Client()
	if err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an RedisPubSub server.
func (r *RedisPubSub) ConnectWithContext(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	client, err := r.conf.Config.Client()
	if err != nil {
		return err
	}
	if _, err := client.Ping().Result(); err != nil {
		return err
	}

	r.log.Infof("Receiving Redis pub/sub messages from channels: %v\n", r.conf.Channels)

	r.client = client
	if r.conf.UsePatterns {
		r.pubsub = r.client.PSubscribe(r.conf.Channels...)
	} else {
		r.pubsub = r.client.Subscribe(r.conf.Channels...)
	}
	return nil
}

// ReadWithContext attempts to pop a message from a redis pubsub channel.
func (r *RedisPubSub) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var pubsub *redis.PubSub

	r.cMut.Lock()
	pubsub = r.pubsub
	r.cMut.Unlock()

	if pubsub == nil {
		return nil, nil, types.ErrNotConnected
	}

	select {
	case rMsg, open := <-pubsub.Channel():
		if !open {
			r.disconnect()
			return nil, nil, types.ErrTypeClosed
		}
		return message.New([][]byte{[]byte(rMsg.Payload)}), noopAsyncAckFn, nil
	case <-ctx.Done():
	}

	return nil, nil, types.ErrTimeout
}

// disconnect safely closes a connection to an RedisPubSub server.
func (r *RedisPubSub) disconnect() error {
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

// CloseAsync shuts down the RedisPubSub input and stops processing requests.
func (r *RedisPubSub) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisPubSub input has closed down.
func (r *RedisPubSub) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
