package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCache] = TypeSpec{
		constructor: fromSimpleConstructor(NewCache),
		Summary:     `Stores each message in a [cache](/docs/components/caches/about).`,
		Description: `Caches are configured as [resources](/docs/components/caches/about), where there's a wide variety to choose from.

The ` + "`target`" + ` field must reference a configured cache resource label like follows:

` + "```yaml" + `
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
` + "```" + `

In order to create a unique ` + "`key`" + ` value per item you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("target", "The target cache to store messages in."),
			docs.FieldCommon("key", "The key to store messages by, function interpolation should be used in order to derive a unique key for each message.",
				`${!count("items")}-${!timestamp_unix_nano()}`,
				`${!json("doc.id")}`,
				`${!meta("kafka_key")}`,
			).IsInterpolated(),
			docs.FieldAdvanced(
				"ttl", "The TTL of each individual item as a duration string. After this period an item will be eligible for removal during the next compaction. Not all caches support per-key TTLs, and those that do not will fall back to their generally configured TTL setting.",
				"60s", "5m", "36h",
			).IsInterpolated().AtVersion("3.33.0"),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewCache creates a new Cache output type.
func NewCache(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	c, err := writer.NewCache(conf.Cache, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncWriter(
		TypeCache, conf.Cache.MaxInFlight, c, log, stats,
	)
}

//------------------------------------------------------------------------------
