package output

import (
	"sort"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
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
		cachesList = cachesList + "- " + k + "\n"
	}

	Constructors[TypeCache] = TypeSpec{
		constructor: NewCache,
		Description: `
Stores message parts as items in a cache. Caches are configured within the
[resources section](../caches/README.md) and can target any of the following
types:

` + cachesList + `
Like follows:

` + "``` yaml" + `
output:
  cache:
    target: foo
    key: ${!json_field:document.id}
resources:
  caches:
    foo:
      memcached:
        addresses:
        - localhost:11211
        ttl: 60
` + "```" + `

In order to create a unique ` + "`key`" + ` value per item you should use
function interpolations described [here](../config_interpolation.md#functions).
When sending batched messages the interpolations are performed per message part.`,
		Async: true,
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
