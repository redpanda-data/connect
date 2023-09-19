package middleware

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"
)

// CacheInitValuesFields return the fields to be added to a cache configuration to support init values.
func CacheInitValuesFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringMapField("init_values").
			Description("A table of key/value pairs that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default(map[string]string{}).
			Example(map[string]string{
				"Nickelback":       "1995",
				"Spice Girls":      "1994",
				"The Human League": "1977",
			}),
	}
}

// ApplyCacheInitValuesFields apply CacheInitValuesFields into config spec.
func ApplyCacheInitValuesFields(spec *service.ConfigSpec) *service.ConfigSpec {
	for _, f := range CacheInitValuesFields() {
		spec = spec.Field(f)
	}

	return spec
}

// WrapCacheInitValues wrap the cache constructor to include values to initialize.
func WrapCacheInitValues(ctx context.Context, ctor service.CacheConstructor) service.CacheConstructor {
	return func(conf *service.ParsedConfig, mgr *service.Resources) (cache service.Cache, err error) {
		var initValues map[string]string

		if conf.Contains("init_values") {
			initValues, err = conf.FieldStringMap("init_values")
			if err != nil {
				return nil, err
			}
		}

		cache, err = ctor(conf, mgr)
		if err != nil {
			return nil, err
		}

		for key, value := range initValues {
			if err := cache.Set(ctx, key, []byte(value), nil); err != nil {
				if cerr := cache.Close(ctx); cerr != nil {
					mgr.Logger().With("error", cerr).Warn("unexpected error while closing cache")
				}

				return nil, err
			}
		}

		return cache, nil
	}
}
