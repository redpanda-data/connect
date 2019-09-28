// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package reader

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

// RedisListConfig contains configuration fields for the RedisList input type.
type RedisListConfig struct {
	URL     string `json:"url" yaml:"url"`
	Key     string `json:"key" yaml:"key"`
	Timeout string `json:"timeout" yaml:"timeout"`
}

// NewRedisListConfig creates a new RedisListConfig with default values.
func NewRedisListConfig() RedisListConfig {
	return RedisListConfig{
		URL:     "tcp://localhost:6379",
		Key:     "benthos_list",
		Timeout: "5s",
	}
}

//------------------------------------------------------------------------------

// RedisList is an input type that reads Redis List messages.
type RedisList struct {
	client *redis.Client
	cMut   sync.Mutex

	url     *url.URL
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

	var err error
	r.url, err = url.Parse(r.conf.URL)
	if err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to a Redis server.
func (r *RedisList) Connect() error {
	return r.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to a Redis server.
func (r *RedisList) ConnectWithContext(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

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

	r.log.Infof("Receiving messages from Redis list: %v\n", r.conf.Key)

	r.client = client
	return nil
}

// Read attempts to pop a message from a Redis list.
func (r *RedisList) Read() (types.Message, error) {
	msg, _, err := r.ReadWithContext(context.Background())
	return msg, err
}

// ReadWithContext attempts to pop a message from a Redis list.
func (r *RedisList) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var client *redis.Client

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

	return message.New([][]byte{[]byte(res[1])}), noopAsyncAckFn, nil
}

// Acknowledge is a noop since Redis Lists do not support acknowledgements.
func (r *RedisList) Acknowledge(err error) error {
	return nil
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
