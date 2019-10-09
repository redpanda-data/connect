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
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis"
)

//------------------------------------------------------------------------------

// RedisStreamsConfig contains configuration fields for the RedisStreams input
// type.
type RedisStreamsConfig struct {
	URL             string             `json:"url" yaml:"url"`
	BodyKey         string             `json:"body_key" yaml:"body_key"`
	Streams         []string           `json:"streams" yaml:"streams"`
	ConsumerGroup   string             `json:"consumer_group" yaml:"consumer_group"`
	ClientID        string             `json:"client_id" yaml:"client_id"`
	Limit           int64              `json:"limit" yaml:"limit"`
	Batching        batch.PolicyConfig `json:"batching" yaml:"batching"`
	StartFromOldest bool               `json:"start_from_oldest" yaml:"start_from_oldest"`
	CommitPeriod    string             `json:"commit_period" yaml:"commit_period"`
	Timeout         string             `json:"timeout" yaml:"timeout"`
}

// NewRedisStreamsConfig creates a new RedisStreamsConfig with default values.
func NewRedisStreamsConfig() RedisStreamsConfig {
	batchConf := batch.NewPolicyConfig()
	batchConf.Count = 1
	return RedisStreamsConfig{
		URL:             "tcp://localhost:6379",
		BodyKey:         "body",
		Streams:         []string{"benthos_stream"},
		ConsumerGroup:   "benthos_group",
		ClientID:        "benthos_consumer",
		Limit:           10,
		Batching:        batchConf,
		StartFromOldest: true,
		CommitPeriod:    "1s",
		Timeout:         "5s",
	}
}

//------------------------------------------------------------------------------

// RedisStreams is an input type that reads Redis Streams messages.
type RedisStreams struct {
	client *redis.Client
	cMut   sync.Mutex

	timeout      time.Duration
	commitPeriod time.Duration

	url  *url.URL
	conf RedisStreamsConfig

	backlogs map[string]string

	aMut       sync.Mutex
	ackSend    map[string][]string // Acks that can be sent
	ackPending map[string][]string // Acks that are pending

	stats metrics.Type
	log   log.Modular

	closeChan  chan struct{}
	closedChan chan struct{}
	closeOnce  sync.Once
}

// NewRedisStreams creates a new RedisStreams input type.
func NewRedisStreams(
	conf RedisStreamsConfig, log log.Modular, stats metrics.Type,
) (*RedisStreams, error) {
	r := &RedisStreams{
		conf:       conf,
		stats:      stats,
		log:        log,
		backlogs:   make(map[string]string, len(conf.Streams)),
		ackSend:    make(map[string][]string, len(conf.Streams)),
		ackPending: make(map[string][]string, len(conf.Streams)),
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	for _, str := range conf.Streams {
		r.backlogs[str] = "0"
	}

	var err error
	r.url, err = url.Parse(r.conf.URL)
	if err != nil {
		return nil, err
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if r.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if tout := conf.CommitPeriod; len(tout) > 0 {
		var err error
		if r.commitPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse commit period string: %v", err)
		}
	}

	go r.loop()
	return r, nil
}

//------------------------------------------------------------------------------

func (r *RedisStreams) loop() {
	defer func() {
		var client *redis.Client
		r.cMut.Lock()
		client = r.client
		r.client = nil
		r.cMut.Unlock()
		if client != nil {
			client.Close()
		}
		close(r.closedChan)
	}()
	commitTimer := time.NewTicker(r.commitPeriod)

	closed := false
	for !closed {
		select {
		case <-commitTimer.C:
		case <-r.closeChan:
			closed = true
		}
		r.sendAcks()
	}
}

func (r *RedisStreams) addPendingAcks(stream string, ids ...string) {
	r.aMut.Lock()
	if acks, exists := r.ackPending[stream]; exists {
		acks = append(acks, ids...)
		r.ackPending[stream] = acks
	} else {
		r.ackPending[stream] = ids
	}
	r.aMut.Unlock()
}

func (r *RedisStreams) addAsyncAcks(stream string, ids ...string) {
	r.aMut.Lock()
	if acks, exists := r.ackSend[stream]; exists {
		acks = append(acks, ids...)
		r.ackSend[stream] = acks
	} else {
		r.ackSend[stream] = ids
	}
	r.aMut.Unlock()
}

func (r *RedisStreams) scheduleAcks() {
	r.aMut.Lock()
	for k, v := range r.ackPending {
		if acks, exists := r.ackSend[k]; exists {
			acks = append(acks, v...)
			r.ackSend[k] = acks
		} else {
			r.ackSend[k] = v
		}
	}
	r.aMut.Unlock()
}

func (r *RedisStreams) sendAcks() {
	var client *redis.Client
	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return
	}

	r.aMut.Lock()
	ackSend := r.ackSend
	r.ackSend = map[string][]string{}
	r.aMut.Unlock()

	for str, ids := range ackSend {
		if len(ids) == 0 {
			continue
		}
		if err := r.client.XAck(str, r.conf.ConsumerGroup, ids...).Err(); err != nil {
			r.log.Errorf("Failed to ack stream %v: %v\n", str, err)
		}
	}
}

//------------------------------------------------------------------------------

// Connect establishes a connection to a Redis server.
func (r *RedisStreams) Connect() error {
	return r.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to a Redis server.
func (r *RedisStreams) ConnectWithContext(ctx context.Context) error {
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

	for _, s := range r.conf.Streams {
		offset := "$"
		if r.conf.StartFromOldest {
			offset = "0"
		}
		if err := client.XGroupCreate(s, r.conf.ConsumerGroup, offset).Err(); err != nil {
			if err.Error() != "BUSYGROUP Consumer Group name already exists" {
				return fmt.Errorf("failed to create group %v for stream %v: %v", s, r.conf.ConsumerGroup, err)
			}
		}
	}

	r.log.Infof("Receiving messages from Redis streams: %v\n", r.conf.Streams)

	r.client = client
	return nil
}

func (r *RedisStreams) read() (types.Message, map[string][]string, error) {
	var client *redis.Client

	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return nil, nil, types.ErrNotConnected
	}

	strs := make([]string, len(r.conf.Streams)*2)
	for i, str := range r.conf.Streams {
		strs[i] = str
		if bl := r.backlogs[str]; bl == "" {
			strs[len(r.conf.Streams)+i] = bl
		} else {
			strs[len(r.conf.Streams)+i] = ">"
		}
	}

	res, err := client.XReadGroup(&redis.XReadGroupArgs{
		Block:    r.timeout,
		Consumer: r.conf.ClientID,
		Group:    r.conf.ConsumerGroup,
		Streams:  strs,
		Count:    r.conf.Limit,
	}).Result()

	if err != nil && err != redis.Nil {
		if strings.Contains(err.Error(), "i/o timeout") {
			return nil, nil, types.ErrTimeout
		}
		r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return nil, nil, types.ErrNotConnected
	}

	pendingAcks := map[string][]string{}
	msg := message.New(nil)
	for _, strRes := range res {
		if _, exists := r.backlogs[strRes.Stream]; exists {
			if len(strRes.Messages) > 0 {
				r.backlogs[strRes.Stream] = strRes.Messages[len(strRes.Messages)-1].ID
			} else {
				delete(r.backlogs, strRes.Stream)
			}
		}
		ids := make([]string, 0, len(strRes.Messages))
		for _, xmsg := range strRes.Messages {
			ids = append(ids, xmsg.ID)

			body, exists := xmsg.Values[r.conf.BodyKey]
			if !exists {
				continue
			}

			var bodyBytes []byte
			switch t := body.(type) {
			case string:
				bodyBytes = []byte(t)
			case []byte:
				bodyBytes = t
			}
			if bodyBytes == nil {
				continue
			}

			part := message.NewPart(bodyBytes)
			part.Metadata().Set("redis_stream", xmsg.ID)
			for k, v := range xmsg.Values {
				part.Metadata().Set(k, fmt.Sprintf("%v", v))
			}

			msg.Append(part)
		}
		if acks, exists := r.ackPending[strRes.Stream]; exists {
			acks = append(acks, ids...)
			pendingAcks[strRes.Stream] = acks
		} else {
			pendingAcks[strRes.Stream] = ids
		}
	}

	if msg.Len() < 1 {
		return nil, nil, types.ErrTimeout
	}

	return msg, pendingAcks, nil
}

// ReadWithContext attempts to pop a message from a Redis list.
func (r *RedisStreams) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	msg, acks, err := r.read()
	if err != nil {
		return nil, nil, err
	}
	return msg, func(rctx context.Context, res types.Response) error {
		if res.Error() != nil {
			r.log.Errorf("Received error from message batch: %v, shutting down consumer.\n", res.Error())
			r.CloseAsync()
			return nil
		}
		for str, ids := range acks {
			r.addAsyncAcks(str, ids...)
		}
		return nil
	}, nil
}

// Read attempts to pop a message from a Redis list.
func (r *RedisStreams) Read() (types.Message, error) {
	msg, acks, err := r.read()
	if err != nil {
		return nil, err
	}
	for str, ids := range acks {
		r.addPendingAcks(str, ids...)
	}
	return msg, nil
}

// Acknowledge is a noop since Redis Lists do not support acknowledgements.
func (r *RedisStreams) Acknowledge(err error) error {
	if err == nil {
		r.scheduleAcks()
	}
	return nil
}

// disconnect safely closes a connection to an RedisStreams server.
func (r *RedisStreams) disconnect() error {
	r.sendAcks()

	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

// CloseAsync shuts down the RedisStreams input and stops processing requests.
func (r *RedisStreams) CloseAsync() {
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})
}

// WaitForClose blocks until the RedisStreams input has closed down.
func (r *RedisStreams) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
