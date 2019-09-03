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
		description: `
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
	}
}

//------------------------------------------------------------------------------

// NewCache creates a new Cache output type.
func NewCache(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	c, err := writer.NewCache(conf.Cache, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		"cache", c, log, stats,
	)
}

//------------------------------------------------------------------------------
