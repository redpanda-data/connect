package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// CacheConfig contains configuration fields for the Cache output type.
type CacheConfig struct {
	Target      string `json:"target" yaml:"target"`
	Key         string `json:"key" yaml:"key"`
	TTL         string `json:"ttl" yaml:"ttl"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewCacheConfig creates a new Config with default values.
func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Target:      "",
		Key:         `${!count("items")}-${!timestamp_unix_nano()}`,
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// Cache is a benthos writer.Type implementation that writes messages to a
// Cache directory.
type Cache struct {
	conf CacheConfig

	key   field.Expression
	ttl   field.Expression
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
	key, err := bloblang.NewField(conf.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}
	ttl, err := bloblang.NewField(conf.TTL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}
	return &Cache{
		conf:  conf,
		key:   key,
		ttl:   ttl,
		cache: cache,
		log:   log,
		stats: stats,
	}, nil
}

// ConnectWithContext does nothing.
func (c *Cache) ConnectWithContext(ctx context.Context) error {
	return c.Connect()
}

// Connect does nothing.
func (c *Cache) Connect() error {
	c.log.Infof("Writing message parts as items in cache: %v\n", c.conf.Target)
	return nil
}

// WriteWithContext attempts to write message contents to a target Cache.
func (c *Cache) WriteWithContext(ctx context.Context, msg types.Message) error {
	return c.Write(msg)
}

// Write attempts to write message contents to a target Cache.
func (c *Cache) Write(msg types.Message) error {
	if msg.Len() == 1 {
		var ttl *time.Duration
		var err error
		if ttls := c.ttl.String(0, msg); ttls != "" {
			*ttl, err = time.ParseDuration(ttls)
			if err != nil {
				c.log.Debugf("TTL must be a duration: %v\n", err)
				return err
			}
		}
		return c.cache.Set(c.key.String(0, msg), msg.Get(0).Get(), ttl)
	}
	items := map[string][]byte{}
	msg.Iter(func(i int, p types.Part) error {
		items[c.key.String(i, msg)] = p.Get()
		return nil
	})
	if len(items) > 0 {
		return c.cache.SetMulti(items, nil)
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
