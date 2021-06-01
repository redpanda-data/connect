package config_test

import (
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	_ "github.com/Jeffail/benthos/v3/public/components/all"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func walkSpecWithConfig(t *testing.T, prefix string, spec docs.FieldSpec, conf interface{}) {
	t.Helper()

	if _, isCore := spec.Type.IsCoreComponent(); isCore {
		return
	}

	if spec.IsArray {
		arr, ok := conf.([]interface{})
		if !assert.True(t, ok || spec.Deprecated, "%v: documented as array but is %T", prefix, conf) {
			return
		}
		for i, ele := range arr {
			tmpSpec := spec
			tmpSpec.IsArray = false
			walkSpecWithConfig(t, prefix+fmt.Sprintf("[%v]", i), tmpSpec, ele)
		}
	} else if spec.IsMap {
		obj, ok := conf.(map[string]interface{})
		if !assert.True(t, ok || spec.Deprecated, "%v: documented as map but is %T", prefix, conf) {
			return
		}
		for k, v := range obj {
			tmpSpec := spec
			tmpSpec.IsMap = false
			walkSpecWithConfig(t, prefix+fmt.Sprintf(".<%v>", k), tmpSpec, v)
		}
	} else if len(spec.Children) > 0 {
		obj, ok := conf.(map[string]interface{})
		if !assert.True(t, ok, "%v: documented with children but is %T", prefix, conf) {
			return
		}
		for _, child := range spec.Children {
			c, ok := obj[child.Name]
			if assert.True(t, ok || child.Deprecated, "%v: field documented but not found in config", prefix+"."+child.Name) {
				walkSpecWithConfig(t, prefix+"."+child.Name, child, c)
			}
			delete(obj, child.Name)
		}
		for k := range obj {
			t.Errorf("%v: field found in config but not documented", prefix+"."+k)
		}
	} else {
		_, isArray := conf.([]interface{})
		assert.False(t, isArray, "%v: documented as scalar but is %T", prefix, conf)

		_, isObj := conf.(map[string]interface{})
		assert.False(t, isObj, "%v: documented as scalar but is %T", prefix, conf)
	}
}

func TestDocumentationCoverage(t *testing.T) {
	t.Run("buffers", func(t *testing.T) {
		for _, v := range bundle.AllBuffers.Docs() {
			conf := buffer.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "buffer."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})

	t.Run("caches", func(t *testing.T) {
		for _, v := range bundle.AllCaches.Docs() {
			conf := cache.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "cache."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})

	t.Run("inputs", func(t *testing.T) {
		for _, v := range bundle.AllInputs.Docs() {
			conf := input.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "input."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})

	t.Run("metrics", func(t *testing.T) {
		for _, v := range bundle.AllMetrics.Docs() {
			conf := metrics.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "metrics."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})

	t.Run("outputs", func(t *testing.T) {
		for _, v := range bundle.AllOutputs.Docs() {
			conf := output.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "output."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})

	t.Run("processors", func(t *testing.T) {
		for _, v := range bundle.AllProcessors.Docs() {
			conf := processor.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "processor."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})

	t.Run("rate limits", func(t *testing.T) {
		for _, v := range bundle.AllRateLimits.Docs() {
			conf := ratelimit.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "rate_limit."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})

	t.Run("tracers", func(t *testing.T) {
		for _, v := range bundle.AllTracers.Docs() {
			conf := tracer.NewConfig()
			conf.Type = v.Name
			confSanit, err := conf.Sanitised(false)
			require.NoError(t, err)
			if !v.Plugin {
				walkSpecWithConfig(t, "tracer."+v.Name, v.Config, confSanit.(config.Sanitised)[v.Name])
			}
		}
	})
}
