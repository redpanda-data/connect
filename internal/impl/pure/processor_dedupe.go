package pure

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	dedupFieldCache          = "cache"
	dedupFieldKey            = "key"
	dedupFieldDropOnCacheErr = "drop_on_err"
)

func dedupeProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`Deduplicates messages by storing a key value in a cache using the `+"`add`"+` operator. If the key already exists within the cache it is dropped.`).
		Description(`
Caches must be configured as resources, for more information check out the [cache documentation here](/docs/components/caches/about).

When using this processor with an output target that might fail you should always wrap the output within an indefinite `+"[`retry`](/docs/components/outputs/retry)"+` block. This ensures that during outages your messages aren't reprocessed after failures, which would result in messages being dropped.

## Batch Deduplication

This processor enacts on individual messages only, in order to perform a deduplication on behalf of a batch (or window) of messages instead use the `+"[`cache` processor](/docs/components/processors/cache#examples)"+`.

## Delivery Guarantees

Performing deduplication on a stream using a distributed cache voids any at-least-once guarantees that it previously had. This is because the cache will preserve message signatures even if the message fails to leave the Benthos pipeline, which would cause message loss in the event of an outage at the output sink followed by a restart of the Benthos instance (or a server crash, etc).

This problem can be mitigated by using an in-memory cache and distributing messages to horizontally scaled Benthos pipelines partitioned by the deduplication key. However, in situations where at-least-once delivery guarantees are important it is worth avoiding deduplication in favour of implement idempotent behaviour at the edge of your stream pipelines.`).
		Example(
			"Deduplicate based on Kafka key",
			"The following configuration demonstrates a pipeline that deduplicates messages based on the Kafka key.",
			`
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
		).
		Fields(
			service.NewStringField(dedupFieldCache).
				Description("The [`cache` resource](/docs/components/caches/about) to target with this processor."),
			service.NewInterpolatedStringField(dedupFieldKey).
				Description("An interpolated string yielding the key to deduplicate by for each message.").
				Examples(`${! meta("kafka_key") }`, `${! content().hash("xxhash64") }`),
			service.NewBoolField(dedupFieldDropOnCacheErr).
				Description("Whether messages should be dropped when the cache returns a general error such as a network issue.").
				Default(true),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"dedupe", dedupeProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			cache, err := conf.FieldString(dedupFieldCache)
			if err != nil {
				return nil, err
			}

			keyStr, err := conf.FieldString(dedupFieldKey)
			if err != nil {
				return nil, err
			}

			dropOnErr, err := conf.FieldBool(dedupFieldDropOnCacheErr)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newDedupe(cache, keyStr, dropOnErr, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("dedupe", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type dedupeProc struct {
	log log.Modular

	dropOnErr bool
	key       *field.Expression
	mgr       bundle.NewManagement
	cacheName string
}

func newDedupe(cache, keyStr string, dropOnErr bool, mgr bundle.NewManagement) (*dedupeProc, error) {
	if keyStr == "" {
		return nil, errors.New("dedupe key must not be empty")
	}
	key, err := mgr.BloblEnvironment().NewField(keyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	if !mgr.ProbeCache(cache) {
		return nil, fmt.Errorf("cache resource '%v' was not found", cache)
	}

	return &dedupeProc{
		log:       mgr.Logger(),
		dropOnErr: dropOnErr,
		key:       key,
		mgr:       mgr,
		cacheName: cache,
	}, nil
}

func (d *dedupeProc) ProcessBatch(ctx *processor.BatchProcContext, batch message.Batch) ([]message.Batch, error) {
	newBatch := message.QuickBatch(nil)
	_ = batch.Iter(func(i int, p *message.Part) error {
		key, err := d.key.String(i, batch)
		if err != nil {
			err = fmt.Errorf("key interpolation error: %w", err)
			ctx.OnError(err, i, nil)
			return nil
		}

		if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(cache cache.V1) {
			err = cache.Add(context.Background(), key, []byte{'t'}, nil)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			if errors.Is(err, component.ErrKeyAlreadyExists) {
				ctx.Span(i).LogKV("dropped", "type", "deduplicated")
				return nil
			}

			d.log.Error("Cache error: %v\n", err)
			if d.dropOnErr {
				ctx.Span(i).LogKV("dropped", "type", "deduplicated")
				return nil
			}

			ctx.OnError(err, i, p)
		}

		newBatch = append(newBatch, p)
		return nil
	})

	if newBatch.Len() == 0 {
		return nil, nil
	}
	return []message.Batch{newBatch}, nil
}

func (d *dedupeProc) Close(context.Context) error {
	return nil
}
