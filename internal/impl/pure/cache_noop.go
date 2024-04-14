package pure

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

var _ service.Cache = (*noopCacheAdapter)(nil)

func noopCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Version("4.27.0").
		Summary("Noop is a cache that stores nothing, all gets returns not found. Why? Sometimes doing nothing is the braver option.").
		Field(service.NewObjectField("").Default(map[string]any{}))

	return spec
}

func init() {
	err := service.RegisterCache(
		"noop", noopCacheConfig(),
		func(_ *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f := noopMemCache(mgr.Label(), mgr.Logger())

			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func noopMemCache(label string, logger *service.Logger) *noopCacheAdapter {
	return &noopCacheAdapter{
		logger: logger.With("cache", "noop", "label", label),
	}
}

type noopCacheAdapter struct {
	logger *service.Logger
}

func (c *noopCacheAdapter) Add(_ context.Context, key string, _ []byte, _ *time.Duration) error {
	c.logger.Tracef("pretend to add key %q", key)

	return nil
}

func (c *noopCacheAdapter) Set(_ context.Context, key string, _ []byte, _ *time.Duration) error {
	c.logger.Tracef("pretend to set key %q", key)

	return nil
}

func (c *noopCacheAdapter) Delete(_ context.Context, key string) error {
	c.logger.Tracef("pretend to delete key %q", key)

	return nil
}

func (c *noopCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	c.logger.Tracef("pretend to get key %q", key)

	return nil, service.ErrKeyNotFound
}

func (c *noopCacheAdapter) Close(context.Context) error {
	c.logger.Debug("close cache")

	return nil
}
