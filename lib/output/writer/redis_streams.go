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

package writer

import (
	"net/url"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

// RedisStreamsConfig contains configuration fields for the RedisStreams output type.
type RedisStreamsConfig struct {
	URL          string `json:"url" yaml:"url"`
	Stream       string `json:"stream" yaml:"stream"`
	BodyKey      string `json:"body_key" yaml:"body_key"`
	MaxLenApprox int64  `json:"max_length" yaml:"max_length"`
}

// NewRedisStreamsConfig creates a new RedisStreamsConfig with default values.
func NewRedisStreamsConfig() RedisStreamsConfig {
	return RedisStreamsConfig{
		URL:          "tcp://localhost:6379",
		Stream:       "benthos_stream",
		BodyKey:      "body",
		MaxLenApprox: 0,
	}
}

//------------------------------------------------------------------------------

// RedisStreams is an output type that serves RedisStreams messages.
type RedisStreams struct {
	log   log.Modular
	stats metrics.Type

	url  *url.URL
	conf RedisStreamsConfig

	client  *redis.Client
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
	r.url, err = url.Parse(conf.URL)
	if err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an RedisStreams server.
func (r *RedisStreams) Connect() error {
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

	r.log.Infof("Pushing messages to Redis stream: %v\n", r.conf.Stream)

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

// Write attempts to write a message by pushing it to the end of a Redis list.
func (r *RedisStreams) Write(msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		values := map[string]interface{}{}
		p.Metadata().Iter(func(k, v string) error {
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
