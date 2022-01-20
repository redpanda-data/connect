package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCache] = TypeSpec{
		constructor: NewCache,
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Performs operations against a [cache resource](/docs/components/caches/about) for each message, allowing you to store or retrieve data within message payloads.`,
		Description: `
This processor will interpolate functions within the ` + "`key` and `value`" + ` fields individually for each message. This allows you to specify dynamic keys and values based on the contents of the message payloads and metadata. You can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("resource", "The [`cache` resource](/docs/components/caches/about) to target with this processor."),
			docs.FieldCommon("operator", "The [operation](#operators) to perform with the cache.").HasOptions("set", "add", "get", "delete"),
			docs.FieldCommon("key", "A key to use with the cache.").IsInterpolated(),
			docs.FieldCommon("value", "A value to use with the cache (when applicable).").IsInterpolated(),
			docs.FieldAdvanced(
				"ttl", "The TTL of each individual item as a duration string. After this period an item will be eligible for removal during the next compaction. Not all caches support per-key TTLs, and those that do not will fall back to their generally configured TTL setting.",
				"60s", "5m", "36h",
			).IsInterpolated().AtVersion("3.33.0"),
			PartsFieldSpec,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "Deduplication",
				Summary: `
Deduplication can be done using the add operator with a key extracted from the
message payload, since it fails when a key already exists we can remove the
duplicates using a
[` + "`bloblang` processor" + `](/docs/components/processors/bloblang):`,
				Config: `
pipeline:
  processors:
    - cache:
        resource: foocache
        operator: add
        key: '${! json("message.id") }'
        value: "storeme"
    - bloblang: root = if errored() { deleted() }

cache_resources:
  - label: foocache
    redis:
      url: tcp://TODO:6379
`,
			},
			{
				Title: "Hydration",
				Summary: `
It's possible to enrich payloads with content previously stored in a cache by
using the [` + "`branch`" + `](/docs/components/processors/branch) processor:`,
				Config: `
pipeline:
  processors:
    - branch:
        processors:
          - cache:
              resource: foocache
              operator: get
              key: '${! json("message.document_id") }'
        result_map: 'root.message.document = this'

        # NOTE: If the data stored in the cache is not valid JSON then use
        # something like this instead:
        # result_map: 'root.message.document = content().string()'

cache_resources:
  - label: foocache
    memcached:
      addresses: [ "TODO:11211" ]
`,
			},
		},
		Footnotes: `
## Operators

### ` + "`set`" + `

Set a key in the cache to a value. If the key already exists the contents are
overridden.

### ` + "`add`" + `

Set a key in the cache to a value. If the key already exists the action fails
with a 'key already exists' error, which can be detected with
[processor error handling](/docs/configuration/error_handling).

### ` + "`get`" + `

Retrieve the contents of a cached key and replace the original message payload
with the result. If the key does not exist the action fails with an error, which
can be detected with [processor error handling](/docs/configuration/error_handling).

### ` + "`delete`" + `

Delete a key and its contents from the cache.  If the key does not exist the
action is a no-op and will not fail with an error.`,
	}
}

//------------------------------------------------------------------------------

// CacheConfig contains configuration fields for the Cache processor.
type CacheConfig struct {
	Resource string `json:"resource" yaml:"resource"`
	Parts    []int  `json:"parts" yaml:"parts"`
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
	TTL      string `json:"ttl" yaml:"ttl"`
}

// NewCacheConfig returns a CacheConfig with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Resource: "",
		Parts:    []int{},
		Operator: "set",
		Key:      "",
		Value:    "",
		TTL:      "",
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

	key   *field.Expression
	value *field.Expression
	ttl   *field.Expression

	mgr       types.Manager
	cacheName string
	operator  cacheOperator

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
	cacheName := conf.Cache.Resource
	if cacheName == "" {
		return nil, errors.New("cache name must be specified")
	}

	op, err := cacheOperatorFromString(conf.Cache.Operator)
	if err != nil {
		return nil, err
	}

	key, err := interop.NewBloblangField(mgr, conf.Cache.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	value, err := interop.NewBloblangField(mgr, conf.Cache.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	ttl, err := interop.NewBloblangField(mgr, conf.Cache.TTL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}

	if err := interop.ProbeCache(context.Background(), mgr, cacheName); err != nil {
		return nil, err
	}

	return &Cache{
		conf:  conf,
		log:   log,
		stats: stats,

		parts: conf.Cache.Parts,

		key:   key,
		value: value,
		ttl:   ttl,

		mgr:       mgr,
		cacheName: cacheName,
		operator:  op,

		mCount:            stats.GetCounter("count"),
		mErr:              stats.GetCounter("error"),
		mKeyAlreadyExists: stats.GetCounter("key_already_exists"),
		mSent:             stats.GetCounter("sent"),
		mBatchSent:        stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

type cacheOperator func(cache types.Cache, key string, value []byte, ttl *time.Duration) ([]byte, bool, error)

func newCacheSetOperator() cacheOperator {
	return func(cache types.Cache, key string, value []byte, ttl *time.Duration) ([]byte, bool, error) {
		var err error
		if cttl, ok := cache.(types.CacheWithTTL); ok {
			err = cttl.SetWithTTL(key, value, ttl)
		} else {
			err = cache.Set(key, value)
		}
		return nil, false, err
	}
}

func newCacheAddOperator() cacheOperator {
	return func(cache types.Cache, key string, value []byte, ttl *time.Duration) ([]byte, bool, error) {
		var err error
		if cttl, ok := cache.(types.CacheWithTTL); ok {
			err = cttl.AddWithTTL(key, value, ttl)
		} else {
			err = cache.Add(key, value)
		}
		return nil, false, err
	}
}

func newCacheGetOperator() cacheOperator {
	return func(cache types.Cache, key string, _ []byte, _ *time.Duration) ([]byte, bool, error) {
		result, err := cache.Get(key)
		return result, true, err
	}
}

func newCacheDeleteOperator() cacheOperator {
	return func(cache types.Cache, key string, _ []byte, ttl *time.Duration) ([]byte, bool, error) {
		err := cache.Delete(key)
		return nil, false, err
	}
}

func cacheOperatorFromString(operator string) (cacheOperator, error) {
	switch operator {
	case "set":
		return newCacheSetOperator(), nil
	case "add":
		return newCacheAddOperator(), nil
	case "get":
		return newCacheGetOperator(), nil
	case "delete":
		return newCacheDeleteOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", operator)
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Cache) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part types.Part) error {
		key := c.key.String(index, msg)
		value := c.value.Bytes(index, msg)

		var ttl *time.Duration
		if ttls := c.ttl.String(index, msg); ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				c.mErr.Incr(1)
				c.log.Debugf("TTL must be a duration: %v\n", err)
				return err
			}
			ttl = &td
		}

		var result []byte
		var useResult bool
		var err error
		if cerr := interop.AccessCache(context.Background(), c.mgr, c.cacheName, func(cache types.Cache) {
			result, useResult, err = c.operator(cache, key, value, ttl)
		}); cerr != nil {
			err = cerr
		}
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

	IteratePartsWithSpanV2(TypeCache, c.parts, newMsg, proc)

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Cache) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Cache) WaitForClose(_ time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
