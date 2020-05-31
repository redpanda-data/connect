package output

import (
	"sort"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	var cachesList string
	var cachesSlice []string
	for k := range cache.Constructors {
		cachesSlice = append(cachesSlice, k)
	}
	sort.Strings(cachesSlice)
	for _, k := range cachesSlice {
		cachesList = cachesList + "- [`" + k + "`](/docs/components/caches/" + k + "/)\n"
	}

	Constructors[TypeCache] = TypeSpec{
		constructor: NewCache,
		Summary: `
Stores each message in a [cache](/docs/components/caches/about).`,
		Description: `
Caches are configured within the [resources section](/docs/components/caches/about)
and can target any of the following types:

` + cachesList + `
The ` + "`target`" + ` field must point to a configured cache like follows:

` + "``` yaml" + `
output:
  cache:
    target: foo
    key: ${!json("document.id")}

resources:
  caches:
    foo:
      memcached:
        addresses:
          - localhost:11211
        ttl: 60
` + "```" + `

In order to create a unique ` + "`key`" + ` value per item you should use
function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).
When sending batched messages the interpolations are performed per message part.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("target", "The target cache to store messages in."),
			docs.FieldCommon("key", "The key to store messages by, function interpolation should be used in order to derive a unique key for each message.",
				`${!count("items")}-${!timestamp_unix_nano()}`,
				`${!json("doc.id")}`,
				`${!meta("kafka_key")}`,
			).SupportsInterpolation(false),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
	}
}

//------------------------------------------------------------------------------

// NewCache creates a new Cache output type.
func NewCache(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	c, err := writer.NewCache(conf.Cache, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.Cache.MaxInFlight == 1 {
		return NewWriter(
			TypeCache, c, log, stats,
		)
	}
	return NewAsyncWriter(
		TypeCache, conf.Cache.MaxInFlight, c, log, stats,
	)
}

//------------------------------------------------------------------------------
