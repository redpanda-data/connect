package pure

import (
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	coFieldTarget = "target"
	coFieldKey    = "key"
	coFieldTTL    = "ttl"
)

func CacheOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Stores each message in a [cache](/docs/components/caches/about).`).
		Description(output.Description(true, false, `Caches are configured as [resources](/docs/components/caches/about), where there's a wide variety to choose from.

The `+"`target`"+` field must reference a configured cache resource label like follows:

`+"```yaml"+`
output:
  cache:
    target: foo
    key: ${!json("document.id")}

cache_resources:
  - label: foo
    memcached:
      addresses:
        - localhost:11211
      default_ttl: 60s
`+"```"+`

In order to create a unique `+"`key`"+` value per item you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`)).
		Fields(
			service.NewStringField(coFieldTarget).
				Description("The target cache to store messages in."),
			service.NewInterpolatedStringField(coFieldKey).
				Description("The key to store messages by, function interpolation should be used in order to derive a unique key for each message.").
				Examples(
					`${!count("items")}-${!timestamp_unix_nano()}`,
					`${!json("doc.id")}`,
					`${!meta("kafka_key")}`,
				).
				Default(`${!count("items")}-${!timestamp_unix_nano()}`),
			service.NewInterpolatedStringField(coFieldTTL).
				Description("The TTL of each individual item as a duration string. After this period an item will be eligible for removal during the next compaction. Not all caches support per-key TTLs, and those that do not will fall back to their generally configured TTL setting.").
				Examples("60s", "5m", "36h").
				Version("3.33.0").
				Advanced().
				Optional(),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"cache", CacheOutputSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}

			mgr := interop.UnwrapManagement(res)

			var ca *CacheWriter
			if ca, err = NewCacheWriter(conf, mgr); err != nil {
				return
			}

			var s output.Streamed
			if s, err = output.NewAsyncWriter("cache", maxInFlight, ca, mgr); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(s)
			return
		})
	if err != nil {
		panic(err)
	}
}

type CacheWriter struct {
	mgr bundle.NewManagement

	target string
	key    *field.Expression
	ttl    *field.Expression

	log log.Modular
}

// NewCacheWriter creates a writer for cache the output plugin.
func NewCacheWriter(conf *service.ParsedConfig, mgr bundle.NewManagement) (*CacheWriter, error) {
	target, err := conf.FieldString(coFieldTarget)
	if err != nil {
		return nil, err
	}

	keyStr, err := conf.FieldString(coFieldKey)
	if err != nil {
		return nil, err
	}
	key, err := mgr.BloblEnvironment().NewField(keyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	ttlStr, _ := conf.FieldString(coFieldTTL)
	ttl, err := mgr.BloblEnvironment().NewField(ttlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}

	if !mgr.ProbeCache(target) {
		return nil, fmt.Errorf("cache resource '%v' was not found", target)
	}
	return &CacheWriter{
		mgr:    mgr,
		target: target,
		key:    key,
		ttl:    ttl,
		log:    mgr.Logger(),
	}, nil
}

// Connect does nothing.
func (c *CacheWriter) Connect(ctx context.Context) error {
	return nil
}

func (c *CacheWriter) writeMulti(ctx context.Context, msg message.Batch) (err error) {
	items := map[string]cache.TTLItem{}
	if err = msg.Iter(func(i int, p *message.Part) error {
		ttls, terr := c.ttl.String(i, msg)
		if terr != nil {
			return fmt.Errorf("ttl interpolation error: %w", terr)
		}
		var ttl *time.Duration
		if ttls != "" {
			t, terr := time.ParseDuration(ttls)
			if terr != nil {
				c.log.Debug("Invalid duration string for TTL field: %v\n", terr)
				return fmt.Errorf("ttl field: %w", terr)
			}
			ttl = &t
		}
		keyStr, terr := c.key.String(i, msg)
		if terr != nil {
			return fmt.Errorf("key interpolation error: %w", terr)
		}
		items[keyStr] = cache.TTLItem{
			Value: p.AsBytes(),
			TTL:   ttl,
		}
		return nil
	}); err != nil {
		return
	}
	if cerr := c.mgr.AccessCache(ctx, c.target, func(ac cache.V1) {
		err = ac.SetMulti(ctx, items)
	}); cerr != nil {
		err = cerr
	}
	return
}

// WriteBatch attempts to store a message within a cache.
func (c *CacheWriter) WriteBatch(ctx context.Context, msg message.Batch) (err error) {
	if msg.Len() > 1 {
		return c.writeMulti(ctx, msg)
	}
	var key, ttls string
	if key, err = c.key.String(0, msg); err != nil {
		err = fmt.Errorf("key interpolation error: %v", err)
		return
	}
	if ttls, err = c.ttl.String(0, msg); err != nil {
		err = fmt.Errorf("ttl interpolation error: %v", err)
		return
	}
	var ttl *time.Duration
	if ttls != "" {
		t, err := time.ParseDuration(ttls)
		if err != nil {
			c.log.Debug("Invalid duration string for TTL field: %v", err)
			return fmt.Errorf("ttl field: %w", err)
		}
		ttl = &t
	}

	if cerr := c.mgr.AccessCache(ctx, c.target, func(cache cache.V1) {
		err = cache.Set(ctx, key, msg.Get(0).AsBytes(), ttl)
	}); cerr != nil {
		err = cerr
	}
	return
}

// Close does nothing.
func (c *CacheWriter) Close(context.Context) error {
	return nil
}
