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

package writer

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
)

//------------------------------------------------------------------------------

// CacheConfig contains configuration fields for the Cache output type.
type CacheConfig struct {
	Target string `json:"target" yaml:"target"`
	Key    string `json:"key" yaml:"key"`
}

// NewCacheConfig creates a new Config with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Target: "",
		Key:    "${!count:items}-${!timestamp_unix_nano}",
	}
}

//------------------------------------------------------------------------------

// Cache is a benthos writer.Type implementation that writes messages to a
// Cache directory.
type Cache struct {
	conf CacheConfig

	key   *text.InterpolatedString
	cache types.Cache

	log   log.Modular
	stats metrics.Type
}

// NewCache creates a new Cache writer.Type.
func NewCache(
	conf CacheConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*Cache, error) {
	cache, err := mgr.GetCache(conf.Target)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain cache '%v': %v", conf.Target, err)
	}
	return &Cache{
		conf:  conf,
		key:   text.NewInterpolatedString(conf.Key),
		cache: cache,
		log:   log,
		stats: stats,
	}, nil
}

// Connect does nothing.
func (c *Cache) Connect() error {
	c.log.Infof("Writing message parts as items in cache: %v\n", c.conf.Target)
	return nil
}

// Write attempts to write message contents to a target Cache directory as files.
func (c *Cache) Write(msg types.Message) error {
	if msg.Len() == 1 {
		return c.cache.Set(c.key.Get(msg), msg.Get(0).Get())
	}
	items := map[string][]byte{}
	msg.Iter(func(i int, p types.Part) error {
		items[c.key.Get(message.Lock(msg, i))] = p.Get()
		return nil
	})
	if len(items) > 0 {
		return c.cache.SetMulti(items)
	}
	return nil
}

// CloseAsync begins cleaning up resources used by this writer asynchronously.
func (c *Cache) CloseAsync() {
}

// WaitForClose will block until either the writer is closed or a specified
// timeout occurs.
func (c *Cache) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
