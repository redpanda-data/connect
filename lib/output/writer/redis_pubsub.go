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
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

// RedisPubSubConfig contains configuration fields for the RedisPubSub output
// type.
type RedisPubSubConfig struct {
	URL     string `json:"url" yaml:"url"`
	Channel string `json:"channel" yaml:"channel"`
}

// NewRedisPubSubConfig creates a new RedisPubSubConfig with default values.
func NewRedisPubSubConfig() RedisPubSubConfig {
	return RedisPubSubConfig{
		URL:     "tcp://localhost:6379",
		Channel: "benthos_chan",
	}
}

//------------------------------------------------------------------------------

// RedisPubSub is an output type that serves RedisPubSub messages.
type RedisPubSub struct {
	log   log.Modular
	stats metrics.Type

	url        *url.URL
	conf       RedisPubSubConfig
	channelStr *text.InterpolatedString

	client  *redis.Client
	connMut sync.RWMutex
}

// NewRedisPubSub creates a new RedisPubSub output type.
func NewRedisPubSub(
	conf RedisPubSubConfig,
	log log.Modular,
	stats metrics.Type,
) (*RedisPubSub, error) {
	r := &RedisPubSub{
		log:        log,
		stats:      stats,
		conf:       conf,
		channelStr: text.NewInterpolatedString(conf.Channel),
	}

	var err error
	r.url, err = url.Parse(conf.URL)
	if err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an RedisPubSub server.
func (r *RedisPubSub) Connect() error {
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

	r.log.Infof("Pushing messages to Redis channel: %v\n", r.conf.Channel)

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

// Write attempts to write a message by pushing it to the end of a Redis list.
func (r *RedisPubSub) Write(msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		channel := r.channelStr.Get(message.Lock(msg, i))
		if err := client.Publish(channel, p.Get()).Err(); err != nil {
			r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return types.ErrNotConnected
		}
		return nil
	})
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
