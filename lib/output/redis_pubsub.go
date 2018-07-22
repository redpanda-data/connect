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

package output

import (
	"net/url"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["redis_pubsub"] = TypeSpec{
		constructor: NewRedisPubSub,
		description: `
Publishes messages through the Redis PubSub model. It is not possible to
guarantee that messages have been received.`,
	}
}

//------------------------------------------------------------------------------

// RedisPubSubConfig is configuration for the RedisPubSub output type.
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
	running int32

	log   log.Modular
	stats metrics.Type

	url  *url.URL
	conf Config

	client *redis.Client

	transactions <-chan types.Transaction

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewRedisPubSub creates a new RedisPubSub output type.
func NewRedisPubSub(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	r := &RedisPubSub{
		running:    1,
		log:        log.NewModule(".output.redis_pubsub"),
		stats:      stats,
		conf:       conf,
		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}

	var err error
	r.url, err = url.Parse(conf.RedisPubSub.URL)
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
	return nil
}

// disconnect safely closes a connection to an RedisPubSub server.
func (r *RedisPubSub) disconnect() error {
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (r *RedisPubSub) loop() {
	var (
		mRunning   = r.stats.GetCounter("output.redis_pubsub.running")
		mReconErr  = r.stats.GetCounter("output.redis_pubsub.reconnect.error")
		mReconSucc = r.stats.GetCounter("output.redis_pubsub.reconnect.success")
		mCount     = r.stats.GetCounter("output.redis_pubsub.count")
		mSucc      = r.stats.GetCounter("output.redis_pubsub.send.success")
		mErr       = r.stats.GetCounter("output.redis_pubsub.send.error")
	)

	defer func() {
		atomic.StoreInt32(&r.running, 0)

		if err := r.disconnect(); err != nil {
			r.log.Errorf("Failed to disconnect redis client: %v\n", err)
		}
		mRunning.Decr(1)

		close(r.closedChan)
	}()
	mRunning.Incr(1)

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
	r.log.Infof("Sending RedisPubSub messages to URL: %s\n", r.conf.RedisPubSub.URL)

	var open bool
	for atomic.LoadInt32(&r.running) == 1 {
		for r.client == nil {
			r.log.Warnln("Lost RedisPubSub connection, attempting to reconnect.")
			if err := r.connect(); err != nil {
				mReconErr.Incr(1)
				select {
				case <-time.After(time.Second):
				case <-r.closeChan:
					return
				}
			} else {
				r.log.Warnln("Successfully reconnected to RedisPubSub.")
				mReconSucc.Incr(1)
			}
		}

		var ts types.Transaction
		select {
		case ts, open = <-r.transactions:
			if !open {
				return
			}
		case <-r.closeChan:
			return
		}

		mCount.Incr(1)
		var err error
		for _, part := range ts.Payload.GetAll() {
			if _, err = r.client.Publish(r.conf.RedisPubSub.Channel, part).Result(); err == nil {
				mSucc.Incr(1)
			} else {
				r.disconnect()
				mErr.Incr(1)
				break
			}
		}

		select {
		case ts.ResponseChan <- types.NewSimpleResponse(err):
		case <-r.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (r *RedisPubSub) Consume(ts <-chan types.Transaction) error {
	if r.transactions != nil {
		return types.ErrAlreadyStarted
	}
	r.transactions = ts
	go r.loop()
	return nil
}

// CloseAsync shuts down the RedisPubSub output and stops processing messages.
func (r *RedisPubSub) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
	}
}

// WaitForClose blocks until the RedisPubSub output has closed down.
func (r *RedisPubSub) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
