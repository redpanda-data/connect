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

package condition

import (
  "bytes"
  "fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
  "github.com/Jeffail/benthos/v3/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCache] = TypeSpec{
		constructor: NewCache,
		description: `
Returns true if a key exists in a [cache resource](../caches), and
optionally matches a given value. This condition is useful for taking
alternative processing paths based on cache content.

This processor will interpolate functions within the ` + "`key` and `value`" + `
fields individually for each message of the batch. This allows you to specify
dynamic keys and values based on the contents of the message payloads and
metadata. You can find a list of functions
[here](../config_interpolation.md#functions).

### Operators

#### ` + "`hit`" + `

True if the key is present in the cache.

#### ` + "`miss`" + `

True if the key is not present in the cache.

#### ` + "`equals`" + `

True if the key is present in the cache, and the value matches the
cache contents for the key.

`,
	}
}

//------------------------------------------------------------------------------

// CacheConfig is a configuration struct containing fields for the
// cache condition.
type CacheConfig struct {
  Cache    string `json:"cache" yaml:"cache"`
	Part     int    `json:"part" yaml:"part"`
  Operator string `json:"operator" yaml:"operator"`
  Key      string `json:"key" yaml:"key"`
  Value    string `json:"value" yaml:"value"`
}

// NewCacheConfig returns a CacheConfig with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
    Cache:    "",
		Part:     0,
    Operator: "hit",
    Key:      "",
    Value:    "",
	}
}

//------------------------------------------------------------------------------

// Cache is a condition that checks for presence or absense of a key in
// a cache resource.
type Cache struct {
	part  int
  key   *text.InterpolatedString
  value *text.InterpolatedBytes

  cache    types.Cache
  operator cacheOperator

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewCache returns a Cache condition.
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
		part:  conf.Cache.Part,
		key:   text.NewInterpolatedString(conf.Cache.Key),
    value: text.NewInterpolatedBytes([]byte(conf.Cache.Value)),

    cache:    c,
    operator: op,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

type cacheOperator func(key string, value []byte) (bool)

func newCacheHitOperator(cache types.Cache) cacheOperator {
  return func(key string, value []byte) (bool) {
    _, err := cache.Get(key)
    return err == nil
  }
}

func newCacheMissOperator(cache types.Cache) cacheOperator {
  return func(key string, value []byte) (bool) {
    _, err := cache.Get(key)
    return err == types.ErrKeyNotFound
  }
}

func newCacheEqualsOperator(cache types.Cache) cacheOperator {
  return func(key string, value []byte) (bool) {
    cacheValue, err := cache.Get(key)
    if err != nil {
      return false
    }
    return bytes.Compare(cacheValue, value) == 0
  }
}

func cacheOperatorFromString(operator string, cache types.Cache) (cacheOperator, error) {
  switch operator {
  case "hit":
    return newCacheHitOperator(cache), nil
  case "miss":
    return newCacheMissOperator(cache), nil
  case "equal":
  case "equals":
    return newCacheEqualsOperator(cache), nil
  }
  return nil, fmt.Errorf("operator not recognised: %v", operator)
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Cache) Check(msg types.Message) bool {
	c.mCount.Incr(1)

  key := c.key.Get(message.Lock(msg, c.part))
  value := c.value.Get(message.Lock(msg, c.part))

  if c.operator(key, value) {
	  c.mTrue.Incr(1)
    return true
  }
  c.mFalse.Incr(1)
  return false
}

//------------------------------------------------------------------------------
