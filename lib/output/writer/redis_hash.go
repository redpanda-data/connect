// Copyright (c) 2019 Ashley Jeffs
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
	"errors"
	"fmt"
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

// RedisHashConfig contains configuration fields for the RedisHash output type.
type RedisHashConfig struct {
	URL            string            `json:"url" yaml:"url"`
	Key            string            `json:"key" yaml:"key"`
	WalkMetadata   bool              `json:"walk_metadata" yaml:"walk_metadata"`
	WalkJSONObject bool              `json:"walk_json_object" yaml:"walk_json_object"`
	Fields         map[string]string `json:"fields" yaml:"fields"`
}

// NewRedisHashConfig creates a new RedisHashConfig with default values.
func NewRedisHashConfig() RedisHashConfig {
	return RedisHashConfig{
		URL:            "tcp://localhost:6379",
		Key:            "",
		WalkMetadata:   false,
		WalkJSONObject: false,
		Fields:         map[string]string{},
	}
}

//------------------------------------------------------------------------------

// RedisHash is an output type that writes hash objects to Redis using the HMSET
// command.
type RedisHash struct {
	log   log.Modular
	stats metrics.Type

	url  *url.URL
	conf RedisHashConfig

	keyStr *text.InterpolatedString
	fields map[string]*text.InterpolatedString

	client  *redis.Client
	connMut sync.RWMutex
}

// NewRedisHash creates a new RedisHash output type.
func NewRedisHash(
	conf RedisHashConfig,
	log log.Modular,
	stats metrics.Type,
) (*RedisHash, error) {
	r := &RedisHash{
		log:    log,
		stats:  stats,
		conf:   conf,
		keyStr: text.NewInterpolatedString(conf.Key),
		fields: map[string]*text.InterpolatedString{},
	}

	for k, v := range conf.Fields {
		r.fields[k] = text.NewInterpolatedString(v)
	}

	if !conf.WalkMetadata && !conf.WalkJSONObject && len(conf.Fields) == 0 {
		return nil, errors.New("at least one mechanism for setting fields must be enabled")
	}

	var err error
	r.url, err = url.Parse(conf.URL)
	if err != nil {
		return nil, err
	}

	return r, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an RedisHash server.
func (r *RedisHash) Connect() error {
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

	r.log.Infoln("Setting messages as hash objects to Redis")

	r.client = client
	return nil
}

//------------------------------------------------------------------------------

func walkForHashFields(msg types.Message, fields map[string]interface{}) error {
	jVal, err := msg.Get(0).JSON()
	if err != nil {
		return err
	}
	jObj, ok := jVal.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected JSON object, found '%T'", jVal)
	}
	for k, v := range jObj {
		fields[k] = v
	}
	return nil
}

// Write attempts to write a message to Redis by setting it using the HMSET
// command.
func (r *RedisHash) Write(msg types.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		lMsg := message.Lock(msg, i)
		key := r.keyStr.Get(lMsg)
		fields := map[string]interface{}{}
		if r.conf.WalkMetadata {
			p.Metadata().Iter(func(k, v string) error {
				fields[k] = v
				return nil
			})
		}
		if r.conf.WalkJSONObject {
			if err := walkForHashFields(lMsg, fields); err != nil {
				err = fmt.Errorf("failed to walk JSON object: %v", err)
				r.log.Errorf("HMSET error: %v\n", err)
				return err
			}
		}
		for k, v := range r.fields {
			fields[k] = v.Get(lMsg)
		}
		if err := client.HMSet(key, fields).Err(); err != nil {
			r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return types.ErrNotConnected
		}
		return nil
	})
}

// disconnect safely closes a connection to an RedisHash server.
func (r *RedisHash) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

// CloseAsync shuts down the RedisHash output and stops processing messages.
func (r *RedisHash) CloseAsync() {
	r.disconnect()
}

// WaitForClose blocks until the RedisHash output has closed down.
func (r *RedisHash) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
