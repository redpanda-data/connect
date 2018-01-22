// Copyright (c) 2017 Ashley Jeffs
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

package input

import (
	"net/url"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

func init() {
	constructors["redis_pubsub"] = typeSpec{
		constructor: NewRedisPubSub,
		description: `
Redis supports a publish/subscribe model, it's possible to subscribe to multiple
channels using this input.`,
	}
}

//------------------------------------------------------------------------------

// RedisPubSubConfig is configuration for the RedisPubSub input type.
type RedisPubSubConfig struct {
	URL      string   `json:"url" yaml:"url"`
	Channels []string `json:"channels" yaml:"channels"`
}

// NewRedisPubSubConfig creates a new RedisPubSubConfig with default values.
func NewRedisPubSubConfig() RedisPubSubConfig {
	return RedisPubSubConfig{
		URL:      "tcp://localhost:6379",
		Channels: []string{"benthos_chan"},
	}
}

//------------------------------------------------------------------------------

// RedisPubSub is an input type that reads Redis Pub/Sub messages.
type RedisPubSub struct {
	running int32

	client *redis.Client
	pubsub *redis.PubSub

	url   *url.URL
	conf  Config
	stats metrics.Type
	log   log.Modular

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewRedisPubSub creates a new RedisPubSub input type.
func NewRedisPubSub(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	r := &RedisPubSub{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.redis_pubsub"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	var err error
	r.url, err = url.Parse(r.conf.RedisPubSub.URL)
	if err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// connect establishes a connection to an RedisPubSub server.
func (r *RedisPubSub) connect() error {
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

	r.client = client
	r.pubsub = r.client.Subscribe(r.conf.RedisPubSub.Channels...)

	return nil
}

// disconnect safely closes a connection to an RedisPubSub server.
func (r *RedisPubSub) disconnect() error {
	var err error
	if r.pubsub != nil {
		err = r.pubsub.Close()
		r.pubsub = nil
	}
	if err == nil && r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

//------------------------------------------------------------------------------

func (r *RedisPubSub) loop() {
	defer func() {
		atomic.StoreInt32(&r.running, 0)
		if err := r.disconnect(); err != nil {
			r.log.Errorf("Failed to disconnect redis client: %v\n", err)
		}
		r.stats.Decr("input.redis_pubsub.running", 1)

		close(r.messages)
		close(r.closedChan)
	}()
	r.stats.Incr("input.redis_pubsub.running", 1)

	for {
		if err := r.connect(); err != nil {
			r.log.Errorf("Failed to connect to RedisPubSub: %v\n", err)
			select {
			case <-time.After(time.Second):
			case <-r.closeChan:
				return
			}
		} else {
			break
		}
	}
	r.log.Infof("Receiving RedisPubSub messages from URL: %s\n", r.conf.RedisPubSub.URL)

	rcvChan := r.pubsub.Channel()

	var data []byte

	for atomic.LoadInt32(&r.running) == 1 {
		// If no bytes then read a message
		if data == nil {
			select {
			case msg, open := <-rcvChan:
				if !open {
					r.log.Warnln("Lost connection to Redis PubSub, attempting reconnect.")
					r.disconnect()
					if err := r.connect(); err != nil {
						r.log.Warnf("Failed to reconnect: %v\n", err)
						r.stats.Incr("input.redis_pubsub.reconnect.error", 1)
						select {
						case <-time.After(time.Second):
						case <-r.closeChan:
							return
						}
					} else {
						r.log.Warnln("Successfully reconnected to Redis PubSub.")
						r.stats.Incr("input.redis_pubsub.reconnect.success", 1)
						rcvChan = r.pubsub.Channel()
					}
				} else {
					data = []byte(msg.Payload)
				}
			case <-r.closeChan:
				return
			}
			r.stats.Incr("input.redis_pubsub.count", 1)
		}

		// If bytes are read then try and propagate.
		if data != nil {
			select {
			case r.messages <- types.Message{Parts: [][]byte{data}}:
			case <-r.closeChan:
				return
			}
			res, open := <-r.responses
			if !open {
				return
			}
			if resErr := res.Error(); resErr == nil {
				r.stats.Incr("input.redis_pubsub.send.success", 1)
				data = nil
			} else {
				r.stats.Incr("input.redis_pubsub.send.error", 1)
			}
		}
	}

}

// StartListening sets the channel used by the input to validate message
// receipt.
func (r *RedisPubSub) StartListening(responses <-chan types.Response) error {
	if r.responses != nil {
		return types.ErrAlreadyStarted
	}
	r.responses = responses
	go r.loop()
	return nil
}

// MessageChan returns the messages channel.
func (r *RedisPubSub) MessageChan() <-chan types.Message {
	return r.messages
}

// CloseAsync shuts down the RedisPubSub input and stops processing requests.
func (r *RedisPubSub) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
	}
}

// WaitForClose blocks until the RedisPubSub input has closed down.
func (r *RedisPubSub) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
