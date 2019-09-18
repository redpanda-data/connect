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

package processor

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/go-redis/redis"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedis] = TypeSpec{
		constructor: NewRedis,
		description: `
Performs actions against Redis that aren't possible using a
` + "[`cache`](#cache)" + ` processor. Actions are performed for each message of
a batch, where the contents are replaced with the result.

The field ` + "`key`" + ` supports
[interpolation functions](../config_interpolation.md#functions) resolved
individually for each message of the batch.

For example, given payloads with a metadata field ` + "`set_key`" + `, you could
add a JSON field to your payload with the cardinality of their target sets with:

` + "```yaml" + `
- process_field:
    path: meta.cardinality
    result_type: int
    processors:
      - redis:
          url: TODO
          operator: scard
          key: ${!metadata:set_key}
 ` + "```" + `


### Operators

#### ` + "`scard`" + `

Returns the cardinality of a set, or 0 if the key does not exist.

#### ` + "`sadd`" + `

Adds a new member to a set. Returns ` + "`1`" + ` if the member was added.`,
	}
}

//------------------------------------------------------------------------------

// RedisConfig contains configuration fields for the Redis processor.
type RedisConfig struct {
	URL         string `json:"url" yaml:"url"`
	Parts       []int  `json:"parts" yaml:"parts"`
	Operator    string `json:"operator" yaml:"operator"`
	Key         string `json:"key" yaml:"key"`
	Retries     int    `json:"retries" yaml:"retries"`
	RetryPeriod string `json:"retry_period" yaml:"retry_period"`
}

// NewRedisConfig returns a RedisConfig with default values.
func NewRedisConfig() RedisConfig {
	return RedisConfig{
		URL:         "tcp://localhost:6379",
		Parts:       []int{},
		Operator:    "scard",
		Key:         "",
		Retries:     3,
		RetryPeriod: "500ms",
	}
}

//------------------------------------------------------------------------------

// Redis is a processor that performs redis operations
type Redis struct {
	parts []int
	conf  Config
	log   log.Modular
	stats metrics.Type

	key *text.InterpolatedString

	operator    redisOperator
	client      *redis.Client
	retryPeriod time.Duration

	mCount      metrics.StatCounter
	mErr        metrics.StatCounter
	mSent       metrics.StatCounter
	mBatchSent  metrics.StatCounter
	mRedisRetry metrics.StatCounter
}

// NewRedis returns a Redis processor.
func NewRedis(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var retryPeriod time.Duration
	if tout := conf.Redis.RetryPeriod; len(tout) > 0 {
		var err error
		if retryPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse retry period string: %v", err)
		}
	}
	uri, err := url.Parse(conf.Redis.URL)
	if err != nil {
		return nil, err
	}

	var pass string
	if uri.User != nil {
		pass, _ = uri.User.Password()
	}
	client := redis.NewClient(&redis.Options{
		Addr:     uri.Host,
		Network:  uri.Scheme,
		Password: pass,
	})

	r := &Redis{
		parts: conf.Redis.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		key: text.NewInterpolatedString(conf.Redis.Key),

		retryPeriod: retryPeriod,
		client:      client,

		mCount:      stats.GetCounter("count"),
		mErr:        stats.GetCounter("error"),
		mSent:       stats.GetCounter("sent"),
		mBatchSent:  stats.GetCounter("batch.sent"),
		mRedisRetry: stats.GetCounter("redis.retry"),
	}

	if r.operator, err = getRedisOperator(conf.Redis.Operator); err != nil {
		return nil, err
	}
	return r, nil
}

//------------------------------------------------------------------------------

type redisOperator func(r *Redis, key string, value []byte) ([]byte, error)

func newRedisSCardOperator() redisOperator {
	return func(r *Redis, key string, value []byte) ([]byte, error) {
		res, err := r.client.SCard(key).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("SCard command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.SCard(key).Result()
		}

		if err != nil {
			return nil, err
		}
		return strconv.AppendInt(nil, res, 10), nil
	}
}

func newRedisSAddOperator() redisOperator {
	return func(r *Redis, key string, value []byte) ([]byte, error) {
		res, err := r.client.SAdd(key, value).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("SCard command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.SAdd(key, value).Result()
		}

		if err != nil {
			return nil, err
		}
		return strconv.AppendInt(nil, res, 10), nil
	}
}

func getRedisOperator(opStr string) (redisOperator, error) {
	switch opStr {
	case "sadd":
		return newRedisSAddOperator(), nil
	case "scard":
		return newRedisSCardOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (r *Redis) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	r.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		key := r.key.Get(message.Lock(newMsg, index))
		res, err := r.operator(r, key, part.Get())
		if err != nil {
			r.mErr.Incr(1)
			r.log.Debugf("Operator failed for key '%s': %v\n", key, err)
			return err
		}
		part.Set(res)
		return nil
	}

	IteratePartsWithSpan(TypeRedis, r.parts, newMsg, proc)

	r.mBatchSent.Incr(1)
	r.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (r *Redis) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (r *Redis) WaitForClose(timeout time.Duration) error {
	r.client.Close()
	return nil
}

//------------------------------------------------------------------------------
