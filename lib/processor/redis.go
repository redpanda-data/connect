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
Redis is a processor that runs a query against redis and replaces the batch with the result.

The fields` + "`key`" + `and` + "`value`" + `have a support of
[interpolation functions](../config_interpolation.md#functions).

In order to execute a Redis query for each message of the batch use this
processor within a ` + "[`for_each`](#for_each)" + ` processor:

` + "``` yaml" + `
for_each:
- redis:
	operator: scard
	key: ${!content}
` + "```" + `

### Operators

#### ` + "`scard`" + `

Returns the cardinality of a set.

#### ` + "`sadd`" + `

Adds a member in a set. Returns` + "`1`" + `if a member was not in a set and ` + "`0`" + `if a member was.
At the moment, multi insertion is not supported. 
		`,
	}
}

//------------------------------------------------------------------------------

// RedisConfig contains configuration fields for the Redis processor.
type RedisConfig struct {
	URL         string `json:"url" yaml:"url"`
	Parts       []int  `json:"parts" yaml:"parts"`
	Operator    string `json:"operator" yaml:"operator"`
	Key         string `json:"key" yaml:"key"`
	Value       string `json:"value" yaml:"value"`
	Prefix      string `json:"prefix" yaml:"prefix"`
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
		Value:       "",
		Prefix:      "",
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

	key        *text.InterpolatedString
	valueBytes *text.InterpolatedBytes

	operator    redisOperator
	prefix      string
	client      *redis.Client
	retryPeriod time.Duration

	mCount      metrics.StatCounter
	mErr        metrics.StatCounter
	mSent       metrics.StatCounter
	mBatchSent  metrics.StatCounter
	mRedisRetry metrics.StatCounter

	mLatency metrics.StatTimer
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
		parts:  conf.Redis.Parts,
		conf:   conf,
		prefix: conf.Redis.Prefix,
		log:    log,
		stats:  stats,

		key: text.NewInterpolatedString(conf.Redis.Key),
		//valueBytes: text.NewInterpolatedBytes([]byte(conf.Redis.Value)),

		retryPeriod: retryPeriod,
		client:      client,

		mCount:      stats.GetCounter("count"),
		mErr:        stats.GetCounter("error"),
		mSent:       stats.GetCounter("sent"),
		mBatchSent:  stats.GetCounter("batch.sent"),
		mRedisRetry: stats.GetCounter("redis.retry"),

		mLatency: stats.GetTimer("latency"),
	}

	if conf.Redis.Value == "" {
		r.valueBytes = text.NewInterpolatedBytes([]byte("${!content}"))
	} else {
		r.valueBytes = text.NewInterpolatedBytes([]byte(conf.Redis.Value))
	}

	if r.operator, err = getRedisOperator(conf.Redis.Operator, r, "", make([]byte, 0, 0)); err != nil {
		return nil, err
	}
	return r, nil
}

//------------------------------------------------------------------------------

type redisOperator func(r *Redis, key string, value []byte) (string, error)

func newSCardOperator(r *Redis, key string, value []byte) redisOperator {
	return func(r *Redis, key string, value []byte) (string, error) {
		tStarted := time.Now()

		res, err := r.client.SCard(key).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("SCard command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.SCard(key).Result()
		}

		latency := int64(time.Since(tStarted))
		r.mLatency.Timing(latency)

		if err != nil {
			return "", err
		}

		return strconv.FormatInt(int64(res), 10), err
	}
}

// TODO: There natively might be an array of keys.
func newSAddOperator(r *Redis, key string, value []byte) redisOperator {
	return func(r *Redis, key string, value []byte) (string, error) {
		tStarted := time.Now()

		res, err := r.client.SAdd(key, value).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("SCard command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.SAdd(key, value).Result()
		}

		latency := int64(time.Since(tStarted))
		r.mLatency.Timing(latency)

		if err != nil {
			return "", err
		}

		return strconv.FormatInt(int64(res), 10), err
	}
}

func getRedisOperator(opStr string, r *Redis, key string, value []byte) (redisOperator, error) {

	switch opStr {
	case "sadd":
		return newSAddOperator(r, key, value), nil
	case "scard":
		return newSCardOperator(r, key, value), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (r *Redis) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	r.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		key := r.prefix + r.key.Get(message.Lock(newMsg, index))
		value := r.valueBytes.Get(message.Lock(newMsg, index))
		res, err := r.operator(r, key, value)
		if err != nil {
			r.mErr.Incr(1)
			r.log.Debugf("Operator failed for key '%s': %v\n", key, err)
			return err
		}
		part.Set([]byte(res))
		return nil
	}

	IteratePartsWithSpan(TypeRedis, r.parts, newMsg, proc)

	msgs := [1]types.Message{newMsg}

	r.mBatchSent.Incr(1)
	r.mSent.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (r *Redis) CloseAsync() {

}

// WaitForClose blocks until the processor has closed down.
func (r *Redis) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
