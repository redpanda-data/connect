package reader

import (
	"context"
	"fmt"
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

// RedisListConfig contains configuration fields for the RedisList input type.
type RedisListConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Key           string `json:"key" yaml:"key"`
	Timeout       string `json:"timeout" yaml:"timeout"`
}

// NewRedisListConfig creates a new RedisListConfig with default values.
func NewRedisListConfig() RedisListConfig {
	return RedisListConfig{
		Config:  bredis.NewConfig(),
		Key:     "benthos_list",
		Timeout: "5s",
	}
}

//------------------------------------------------------------------------------

// RedisList is an input type that reads Redis List messages.
type RedisList struct {
	client redis.UniversalClient
	cMut   sync.Mutex

	conf    RedisListConfig
	timeout time.Duration

	stats metrics.Type
	log   log.Modular
}

// NewRedisList creates a new RedisList input type.
func NewRedisList(
	conf RedisListConfig, log log.Modular, stats metrics.Type,
) (*RedisList, error) {
	r := &RedisList{
		conf:  conf,
		stats: stats,
		log:   log,
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if r.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if _, err := conf.Config.Client(); err != nil {
		return nil, err
	}
	return r, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to a Redis server.
func (r *RedisList) ConnectWithContext(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	client, err := r.conf.Config.Client()
	if err == nil {
		_, err = client.Ping().Result()
	}
	if err != nil {
		return err
	}

	r.log.Infof("Receiving messages from Redis list: %v\n", r.conf.Key)

	r.client = client
	return nil
}

// ReadWithContext attempts to pop a message from a Redis list.
func (r *RedisList) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	var client redis.UniversalClient

	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return nil, nil, types.ErrNotConnected
	}

	res, err := client.BLPop(r.timeout, r.conf.Key).Result()

	if err != nil && err != redis.Nil {
		r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return nil, nil, types.ErrNotConnected
	}

	if len(res) < 2 {
		return nil, nil, types.ErrTimeout
	}

	return message.QuickBatch([][]byte{[]byte(res[1])}), noopAsyncAckFn, nil
}

// disconnect safely closes a connection to an RedisList server.
func (r *RedisList) disconnect() error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

// CloseAsync shuts down the RedisList input and stops processing requests.
func (r *RedisList) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisList input has closed down.
func (r *RedisList) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
