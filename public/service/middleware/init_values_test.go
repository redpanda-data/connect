package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/benthosdev/benthos/v4/public/service/middleware"
	"github.com/benthosdev/benthos/v4/public/service/servicemock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheInitValuesMiddleware(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		yamlConf string

		buildCtor func(*testing.T) service.CacheConstructor

		errMsg string
	}{
		{
			label:    "should return error if ctor return error",
			yamlConf: ``,
			buildCtor: func(t *testing.T) service.CacheConstructor {
				return func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
					return nil, errors.New("ops")
				}
			},
			errMsg: "ops",
		},
		{
			label:    "should return ctor with no initialization",
			yamlConf: ``,
			buildCtor: func(t *testing.T) service.CacheConstructor {
				mock := servicemock.NewCache(t)
				return func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
					return mock, nil
				}
			},
		},
		{
			label: "should return ctor with successful initialization",
			yamlConf: `---
init_values:
  foo: bar`,
			buildCtor: func(t *testing.T) service.CacheConstructor {
				mock := servicemock.NewCache(t)
				mock.On("Set", context.Background(), "foo", []byte("bar"), (*time.Duration)(nil)).Return(nil)
				return func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
					return mock, nil
				}
			},
		},
		{
			label: "should return ctor with bad initialization and close log",
			yamlConf: `---
init_values:
  foo: bar`,
			buildCtor: func(t *testing.T) service.CacheConstructor {
				mock := servicemock.NewCache(t)
				mock.On("Set", context.Background(), "foo", []byte("bar"), (*time.Duration)(nil)).Return(errors.New("ops"))
				mock.On("Close", context.Background()).Return(nil)
				return func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
					return mock, nil
				}
			},
			errMsg: "ops",
		},
		{
			label: "should return ctor with bad initialization and close log with error",
			yamlConf: `---
init_values:
  foo: bar`,
			buildCtor: func(t *testing.T) service.CacheConstructor {
				mock := servicemock.NewCache(t)
				mock.On("Set", context.Background(), "foo", []byte("bar"), (*time.Duration)(nil)).Return(errors.New("ops"))
				mock.On("Close", context.Background()).Return(errors.New("other error"))
				return func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
					return mock, nil
				}
			},
			errMsg: "ops",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			ctor := tc.buildCtor(t)

			wCtor := middleware.WrapCacheInitValues(ctx, ctor)

			spec := service.NewConfigSpec()
			spec = middleware.ApplyCacheInitValuesFields(spec)
			parsedConfig, err := spec.ParseYAML(tc.yamlConf, nil)
			require.NoError(t, err)

			mgr := service.MockResources()

			_, err = wCtor(parsedConfig, mgr)
			if tc.errMsg != "" {
				assert.EqualError(t, err, tc.errMsg)

				return
			}

			assert.NoError(t, err)
		})
	}

}
