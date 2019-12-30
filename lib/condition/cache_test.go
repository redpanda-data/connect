// Copyright (c) 2019 Ashley Jeffs
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
	"testing"

  "github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestCache(t *testing.T) {

  memCache, err := cache.NewMemory(cache.NewConfig(), nil, log.Noop(), metrics.Noop())
  if err != nil {
    t.Fatal(err)
  }
  memCache.Set("foo", []byte("bar"))

  mgr := &fakeMgr{
    caches: map[string]types.Cache{
      "foocache": memCache,
    },
  }

  tests := []struct {
    name     string
    operator string
    key      string
    value    string
    want     bool
  }{
    {
      name:     "test hit success",
      operator: "hit",
      key:      "foo",
      want:     true,
    },
    {
      name:     "test hit fail",
      operator: "hit",
      key:      "bar",
      want:     false,
    },
    {
      name:     "test miss success",
      operator: "miss",
      key:      "bar",
      want:     true,
    },
    {
      name:     "test miss fail",
      operator: "miss",
      key:      "foo",
      want:     false,
    },
    {
      name:     "test equals success",
      operator: "equals",
      key:      "foo",
      value:    "bar",
      want:     true,
    },
    {
      name:     "test equals key but not value",
      operator: "equals",
      key:      "foo",
      value:    "bim",
      want:     false,
    },
    {
      name:     "test equals fail",
      operator: "equals",
      key:      "bim",
      value:    "foo",
      want:     false,
    },
  }
  for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
      conf := NewConfig()
      conf.Type = "cache"
      conf.Cache.Cache    = "foocache"
      conf.Cache.Operator = tt.operator
      conf.Cache.Key      = tt.key
      conf.Cache.Value    = tt.value

	    c, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	    if err != nil {
		    t.Error(err)
        return
	    }
      if got := c.Check(message.New(nil)); got != tt.want {
		    t.Errorf("Cache.Check() = %v, want %v", got, tt.want)
	    }
    })
  }
}
