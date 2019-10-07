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
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCache] = TypeSpec{
		constructor: NewCache,
		description: `
Performs operations against a [cache resource](../caches) for each message of a
batch, allowing you to store or retrieve data within message payloads.

This processor will interpolate functions within the ` + "`key` and `value`" + `
fields individually for each message of the batch. This allows you to specify
dynamic keys and values based on the contents of the message payloads and
metadata. You can find a list of functions
[here](../config_interpolation.md#functions).

### Operators

#### ` + "`set`" + `

Set a key in the cache to a value. If the key already exists the contents are
overridden.

#### ` + "`add`" + `

Set a key in the cache to a value. If the key already exists the action fails
with a 'key already exists' error, which can be detected with
[processor error handling](../error_handling.md).

#### ` + "`get`" + `

Retrieve the contents of a cached key and replace the original message payload
with the result. If the key does not exist the action fails with an error, which
can be detected with [processor error handling](../error_handling.md).

### Examples

The ` + "`cache`" + ` processor can be used in combination with other processors
in order to solve a variety of data stream problems.

#### Deduplication

Deduplication can be done using the add operator with a key extracted from the
message payload, since it fails when a key already exists we can remove the
duplicates using a
[` + "`processor_failed`" + `](../conditions/README.md#processor_failed)
condition:

` + "``` yaml" + `
- cache:
    cache: TODO
    operator: add
    key: "${!json_field:message.id}"
    value: "storeme"
- filter_parts:
    type: processor_failed
` + "```" + `

#### Hydration

It's possible to enrich payloads with content previously stored in a cache by
using the [` + "`process_map`" + `](#process_map) processor:

` + "``` yaml" + `
- process_map:
    processors:
    - cache:
        cache: TODO
        operator: get
        key: "${!json_field:message.document_id}"
    postmap:
      message.document: .
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// CacheConfig contains configuration fields for the Cache processor.
type CacheConfig struct {
	Cache    string `json:"cache" yaml:"cache"`
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
}

// NewCacheConfig returns a CacheConfig with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Cache:    "",
		Parts:    []int{},
		Operator: "set",
		Key:      "",
		Value:    "",
	}
}

//------------------------------------------------------------------------------

// Cache is a processor that stores or retrieves data from a cache for each
// message of a batch via an interpolated key.
type Cache struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	parts []int

	key   *text.InterpolatedString
	value *text.InterpolatedBytes

	cache    types.Cache
	operator cacheOperator

	mCount            metrics.StatCounter
	mErr              metrics.StatCounter
	mKeyAlreadyExists metrics.StatCounter
	mSent             metrics.StatCounter
	mBatchSent        metrics.StatCounter
}

// NewCache returns a Cache processor.
func NewCache(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	c, err := mgr.GetCache(conf.Cache.Cache)
	if err != nil {
		return nil, err
	}

	op, err := cacheOperatorFromString(conf.Cache.Operator, c)
	if err != nil {
		return nil, err
	}

	return &Cache{
		conf:  conf,
		log:   log,
		stats: stats,

		parts: conf.Cache.Parts,

		key:   text.NewInterpolatedString(conf.Cache.Key),
		value: text.NewInterpolatedBytes([]byte(conf.Cache.Value)),

		cache:    c,
		operator: op,

		mCount:            stats.GetCounter("count"),
		mErr:              stats.GetCounter("error"),
		mKeyAlreadyExists: stats.GetCounter("key_already_exists"),
		mSent:             stats.GetCounter("sent"),
		mBatchSent:        stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

type cacheOperator func(key string, value []byte) ([]byte, bool, error)

func newCacheSetOperator(cache types.Cache) cacheOperator {
	return func(key string, value []byte) ([]byte, bool, error) {
		err := cache.Set(key, value)
		return nil, false, err
	}
}

func newCacheAddOperator(cache types.Cache) cacheOperator {
	return func(key string, value []byte) ([]byte, bool, error) {
		err := cache.Add(key, value)
		return nil, false, err
	}
}

func newCacheGetOperator(cache types.Cache) cacheOperator {
	return func(key string, _ []byte) ([]byte, bool, error) {
		result, err := cache.Get(key)
		return result, true, err
	}
}

func cacheOperatorFromString(operator string, cache types.Cache) (cacheOperator, error) {
	switch operator {
	case "set":
		return newCacheSetOperator(cache), nil
	case "add":
		return newCacheAddOperator(cache), nil
	case "get":
		return newCacheGetOperator(cache), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", operator)
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Cache) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		key := c.key.Get(message.Lock(newMsg, index))
		value := c.value.Get(message.Lock(newMsg, index))

		result, useResult, err := c.operator(key, value)
		if err != nil {
			if err != types.ErrKeyAlreadyExists {
				c.mErr.Incr(1)
				c.log.Debugf("Operator failed for key '%s': %v\n", key, err)
			} else {
				c.mKeyAlreadyExists.Incr(1)
				c.log.Debugf("Key already exists: %v\n", key)
			}
			return err
		}

		if useResult {
			part.Set(result)
		}
		return nil
	}

	IteratePartsWithSpan(TypeCache, c.parts, newMsg, proc)

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Cache) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Cache) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
