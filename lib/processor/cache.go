package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCache] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newCache(conf.Cache, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("cache", p, mgr.Metrics()), nil
		},
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
				"ttl", "The TTL of each individual item as a duration string. After this period an item will be eligible for removal during the next compaction. Not all caches support per-key TTLs, those that do will have a configuration field `default_ttl`, and those that do not will fall back to their generally configured TTL setting.",
				"60s", "5m", "36h",
			).IsInterpolated().AtVersion("3.33.0"),
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
	Operator string `json:"operator" yaml:"operator"`
	Key      string `json:"key" yaml:"key"`
	Value    string `json:"value" yaml:"value"`
	TTL      string `json:"ttl" yaml:"ttl"`
}

// NewCacheConfig returns a CacheConfig with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Resource: "",
		Operator: "set",
		Key:      "",
		Value:    "",
		TTL:      "",
	}
}

//------------------------------------------------------------------------------

type cacheProc struct {
	key   *field.Expression
	value *field.Expression
	ttl   *field.Expression

	mgr       interop.Manager
	cacheName string
	operator  cacheOperator
}

func newCache(conf CacheConfig, mgr interop.Manager) (*cacheProc, error) {
	cacheName := conf.Resource
	if cacheName == "" {
		return nil, errors.New("cache name must be specified")
	}

	op, err := cacheOperatorFromString(conf.Operator)
	if err != nil {
		return nil, err
	}

	key, err := mgr.BloblEnvironment().NewField(conf.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	value, err := mgr.BloblEnvironment().NewField(conf.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	ttl, err := mgr.BloblEnvironment().NewField(conf.TTL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}

	if !mgr.ProbeCache(cacheName) {
		return nil, fmt.Errorf("cache resource '%v' was not found", cacheName)
	}

	return &cacheProc{
		key:   key,
		value: value,
		ttl:   ttl,

		mgr:       mgr,
		cacheName: cacheName,
		operator:  op,
	}, nil
}

//------------------------------------------------------------------------------

type cacheOperator func(ctx context.Context, cache cache.V1, key string, value []byte, ttl *time.Duration) ([]byte, bool, error)

func newCacheSetOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, value []byte, ttl *time.Duration) ([]byte, bool, error) {
		err := cache.Set(ctx, key, value, ttl)
		return nil, false, err
	}
}

func newCacheAddOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, value []byte, ttl *time.Duration) ([]byte, bool, error) {
		err := cache.Add(ctx, key, value, ttl)
		return nil, false, err
	}
}

func newCacheGetOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, _ []byte, _ *time.Duration) ([]byte, bool, error) {
		result, err := cache.Get(ctx, key)
		return result, true, err
	}
}

func newCacheDeleteOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, _ []byte, ttl *time.Duration) ([]byte, bool, error) {
		err := cache.Delete(ctx, key)
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

func (c *cacheProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	resMsg := msg.Copy()
	_ = resMsg.Iter(func(index int, part *message.Part) error {
		key := c.key.String(index, msg)
		value := c.value.Bytes(index, msg)

		var ttl *time.Duration
		if ttls := c.ttl.String(index, msg); ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				c.mgr.Logger().Debugf("TTL must be a duration: %v\n", err)
				processor.MarkErr(part, spans[index], err)
				return nil
			}
			ttl = &td
		}

		var result []byte
		var useResult bool
		var err error
		if cerr := c.mgr.AccessCache(context.Background(), c.cacheName, func(cache cache.V1) {
			result, useResult, err = c.operator(context.Background(), cache, key, value, ttl)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			if err != component.ErrKeyAlreadyExists {
				c.mgr.Logger().Debugf("Operator failed for key '%s': %v\n", key, err)
			} else {
				c.mgr.Logger().Debugf("Key already exists: %v\n", key)
			}
			processor.MarkErr(part, spans[index], err)
			return nil
		}

		if useResult {
			part.Set(result)
		}
		return nil
	})

	return []*message.Batch{resMsg}, nil
}

func (c *cacheProc) Close(ctx context.Context) error {
	return nil
}
