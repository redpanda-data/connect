package processor

import (
	"context"
	"errors"
	"fmt"

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
	Constructors[TypeDedupe] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newDedupe(conf.Dedupe, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("dedupe", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `Deduplicates messages by storing a key value in a cache using the ` + "`add`" + ` operator. If the message already exists within the cache it is dropped.`,
		Description: `
Caches must be configured as resources, for more information check out the [cache documentation here](/docs/components/caches/about).

When using this processor with an output target that might fail you should always wrap the output within an indefinite ` + "[`retry`](/docs/components/outputs/retry)" + ` block. This ensures that during outages your messages aren't reprocessed after failures, which would result in messages being dropped.

## Delivery Guarantees

Performing deduplication on a stream using a distributed cache voids any at-least-once guarantees that it previously had. This is because the cache will preserve message signatures even if the message fails to leave the Benthos pipeline, which would cause message loss in the event of an outage at the output sink followed by a restart of the Benthos instance (or a server crash, etc).

This problem can be mitigated by using an in-memory cache and distributing messages to horizontally scaled Benthos pipelines partitioned by the deduplication key. However, in situations where at-least-once delivery guarantees are important it is worth avoiding deduplication in favour of implement idempotent behaviour at the edge of your stream pipelines.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString("cache", "The [`cache` resource](/docs/components/caches/about) to target with this processor."),
			docs.FieldString("key", "An interpolated string yielding the key to deduplicate by for each message.", `${! meta("kafka_key") }`, `${! content().hash("xxhash64") }`).IsInterpolated(),
			docs.FieldBool("drop_on_err", "Whether messages should be dropped when the cache returns a general error such as a network issue."),
		},
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Deduplicate based on Kafka key",
				Summary: "The following configuration demonstrates a pipeline that deduplicates messages based on the Kafka key.",
				Config: `
pipeline:
  processors:
    - dedupe:
        cache: keycache
        key: ${! meta("kafka_key") }

cache_resources:
  - label: keycache
    memory:
      default_ttl: 60s
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// DedupeConfig contains configuration fields for the Dedupe processor.
type DedupeConfig struct {
	Cache          string `json:"cache" yaml:"cache"`
	Key            string `json:"key" yaml:"key"`
	DropOnCacheErr bool   `json:"drop_on_err" yaml:"drop_on_err"`
}

// NewDedupeConfig returns a DedupeConfig with default values.
func NewDedupeConfig() DedupeConfig {
	return DedupeConfig{
		Cache:          "",
		Key:            "",
		DropOnCacheErr: true,
	}
}

//------------------------------------------------------------------------------

type dedupeProc struct {
	log log.Modular

	dropOnErr bool
	key       *field.Expression
	mgr       interop.Manager
	cacheName string
}

func newDedupe(conf DedupeConfig, mgr interop.Manager) (*dedupeProc, error) {
	if conf.Key == "" {
		return nil, errors.New("dedupe key must not be empty")
	}
	key, err := mgr.BloblEnvironment().NewField(conf.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	if !mgr.ProbeCache(conf.Cache) {
		return nil, fmt.Errorf("cache resource '%v' was not found", conf.Cache)
	}

	return &dedupeProc{
		log:       mgr.Logger(),
		dropOnErr: conf.DropOnCacheErr,
		key:       key,
		mgr:       mgr,
		cacheName: conf.Cache,
	}, nil
}

//------------------------------------------------------------------------------

func (d *dedupeProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, batch *message.Batch) ([]*message.Batch, error) {
	newBatch := message.QuickBatch(nil)
	_ = batch.Iter(func(i int, p *message.Part) error {
		key := d.key.String(i, batch)

		var err error
		if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(cache cache.V1) {
			err = cache.Add(context.Background(), key, []byte{'t'}, nil)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			if err == component.ErrKeyAlreadyExists {
				spans[i].LogKV(
					"event", "dropped",
					"type", "deduplicated",
				)
				return nil
			}

			d.log.Errorf("Cache error: %v\n", err)
			if d.dropOnErr {
				spans[i].LogKV(
					"event", "dropped",
					"type", "deduplicated",
				)
				return nil
			}

			p = p.Copy()
			processor.MarkErr(p, spans[i], err)
		}

		newBatch.Append(p)
		return nil
	})

	if newBatch.Len() == 0 {
		return nil, nil
	}
	return []*message.Batch{newBatch}, nil
}

func (d *dedupeProc) Close(context.Context) error {
	return nil
}
