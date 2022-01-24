package config_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	_ "github.com/Jeffail/benthos/v3/public/components/all"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func walkSpecWithConfig(t *testing.T, prefix string, spec docs.FieldSpec, conf interface{}) {
	t.Helper()

	if _, isCore := spec.Type.IsCoreComponent(); isCore {
		return
	}

	if spec.Kind == docs.Kind2DArray {
		arr, ok := conf.([]interface{})
		if !assert.True(t, ok || spec.IsDeprecated, "%v: documented as array but is %T", prefix, conf) {
			return
		}
		for i, ele := range arr {
			tmpSpec := spec
			tmpSpec.Kind = docs.KindArray
			walkSpecWithConfig(t, prefix+fmt.Sprintf("[%v]", i), tmpSpec, ele)
		}
	} else if spec.Kind == docs.KindArray {
		arr, ok := conf.([]interface{})
		if !assert.True(t, ok || spec.IsDeprecated, "%v: documented as array but is %T", prefix, conf) {
			return
		}
		for i, ele := range arr {
			tmpSpec := spec
			tmpSpec.Kind = docs.KindScalar
			walkSpecWithConfig(t, prefix+fmt.Sprintf("[%v]", i), tmpSpec, ele)
		}
	} else if spec.Kind == docs.KindMap {
		obj, ok := conf.(map[string]interface{})
		if !assert.True(t, ok || spec.IsDeprecated, "%v: documented as map but is %T", prefix, conf) {
			return
		}
		for k, v := range obj {
			tmpSpec := spec
			tmpSpec.Kind = docs.KindScalar
			walkSpecWithConfig(t, prefix+fmt.Sprintf(".<%v>", k), tmpSpec, v)
		}
	} else if len(spec.Children) > 0 {
		obj, ok := conf.(map[string]interface{})
		if !assert.True(t, ok, "%v: documented with children but is %T", prefix, conf) {
			return
		}
		for _, child := range spec.Children {
			c, ok := obj[child.Name]
			if assert.True(t, ok || child.IsDeprecated, "%v: field documented but not found in config", prefix+"."+child.Name) {
				walkSpecWithConfig(t, prefix+"."+child.Name, child, c)
			}
			delete(obj, child.Name)
		}
		if !spec.IsDeprecated {
			for k := range obj {
				t.Errorf("%v: field found in config but not documented", prefix+"."+k)
			}
		}
	} else if spec.Type == docs.FieldTypeObject {
		obj, ok := conf.(map[string]interface{})
		if !assert.True(t, ok || conf == nil, "%v: documented as object but is %T", prefix, conf) {
			return
		}
		if len(obj) > 0 && !spec.IsDeprecated {
			childKeys := []string{}
			for k := range obj {
				childKeys = append(childKeys, k)
			}
			t.Errorf("%v: documented as object with no children but has: %v", prefix, childKeys)
		}
	} else {
		var isCorrect bool
		switch spec.Type {
		case docs.FieldTypeBool:
			_, isCorrect = conf.(bool)
		case docs.FieldTypeString:
			_, isCorrect = conf.(string)
		case docs.FieldTypeFloat, docs.FieldTypeInt:
			switch conf.(type) {
			case int64, int, float64:
				isCorrect = true
			}
		case docs.FieldTypeUnknown:
			isCorrect = true
		default:
			isCorrect = true
		}
		assert.True(t, isCorrect || spec.IsDeprecated, "%v: documented as %v but is %T", prefix, spec.Type, conf)
	}
}

func getFieldByYAMLTag(t reflect.Type, tag string) (reflect.Type, bool) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		yTag := field.Tag.Get("yaml")
		if yTag == tag {
			return field.Type, true
		}
	}
	return nil, false
}

func getFieldsByYAMLTag(t reflect.Type) map[string]reflect.Type {
	tagToField := map[string]reflect.Type{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tagV := field.Tag.Get("yaml")
		if tagV == ",inline" {
			for k, v := range getFieldsByYAMLTag(field.Type) {
				tagToField[k] = v
			}
		} else if tagV != "" {
			tagV = strings.TrimSuffix(tagV, ",omitempty")
			tagToField[tagV] = field.Type
		}
	}
	return tagToField
}

func walkTypeWithConfig(t *testing.T, prefix string, spec docs.FieldSpec, v reflect.Type) {
	t.Helper()

	if _, isCore := spec.Type.IsCoreComponent(); isCore {
		return
	}

	if spec.Kind == docs.Kind2DArray {
		if !assert.True(t, v.Kind() == reflect.Slice, "%v: documented as array but is %v", prefix, v.Kind()) {
			return
		}
		eleSpec := spec
		eleSpec.Kind = docs.KindArray
		walkTypeWithConfig(t, prefix+"[]", eleSpec, v.Elem())
	} else if spec.Kind == docs.KindArray {
		if !assert.True(t, v.Kind() == reflect.Slice, "%v: documented as array but is %v", prefix, v.Kind()) {
			return
		}
		eleSpec := spec
		eleSpec.Kind = docs.KindScalar
		walkTypeWithConfig(t, prefix+"[]", eleSpec, v.Elem())
	} else if spec.Kind == docs.KindMap {
		if !assert.True(t, v.Kind() == reflect.Map, "%v: documented as map but is %v", prefix, v.Kind()) {
			return
		}
		eleSpec := spec
		eleSpec.Kind = docs.KindScalar
		walkTypeWithConfig(t, prefix+"<>", eleSpec, v.Elem())
	} else if len(spec.Children) > 0 {
		fieldByYAMLTag := getFieldsByYAMLTag(v)
		for _, child := range spec.Children {
			field, ok := fieldByYAMLTag[child.Name]
			if assert.True(t, ok, "%v: field documented but not found in config", prefix+"."+child.Name) {
				walkTypeWithConfig(t, prefix+"."+child.Name, child, field)
			}
			delete(fieldByYAMLTag, child.Name)
		}
		for k := range fieldByYAMLTag {
			t.Errorf("%v: field found in config but not documented", prefix+"."+k)
		}
	} else if spec.Type == docs.FieldTypeObject {
		if !assert.True(t, v.Kind() == reflect.Map || v.Kind() == reflect.Struct, "%v: documented as map but is %v", prefix, v.Kind()) {
			return
		}
		if v.Kind() == reflect.Struct && !spec.IsDeprecated {
			for i := 0; i < v.NumField(); i++ {
				t.Errorf("%v: documented as object with no children but has: %v", prefix, v.Field(i).Tag.Get("yaml"))
			}
		}
	} else {
		var isCorrect bool
		switch spec.Type {
		case docs.FieldTypeBool:
			isCorrect = v.Kind() == reflect.Bool
		case docs.FieldTypeString:
			isCorrect = v.Kind() == reflect.String
		case docs.FieldTypeFloat:
			isCorrect = v.Kind() == reflect.Float64 ||
				v.Kind() == reflect.Float32
		case docs.FieldTypeInt:
			isCorrect = v.Kind() == reflect.Int ||
				v.Kind() == reflect.Int64 ||
				v.Kind() == reflect.Int32 ||
				v.Kind() == reflect.Int16 ||
				v.Kind() == reflect.Int8 ||
				v.Kind() == reflect.Uint64 ||
				v.Kind() == reflect.Uint32 ||
				v.Kind() == reflect.Uint16 ||
				v.Kind() == reflect.Uint8
		case docs.FieldTypeUnknown:
			isCorrect = v.Kind() == reflect.Interface
		default:
			isCorrect = false
		}
		assert.True(t, isCorrect || spec.IsDeprecated, "%v: documented as %v but is %v", prefix, spec.Type, v.Kind())

		if _, isCore := spec.Type.IsCoreComponent(); !isCore && !spec.IsDeprecated {
			assert.NotNil(t, spec.Default, "%v: struct config fields should always have a default", prefix)
		}
	}
}

func getGenericConf(t *testing.T, cType docs.Type, c interface{}) map[string]interface{} {
	t.Helper()

	var newNode yaml.Node
	require.NoError(t, newNode.Encode(c))
	require.NoError(t, docs.SanitiseYAML(cType, &newNode, docs.SanitiseConfig{
		RemoveTypeField: true,
	}))

	var gen map[string]interface{}
	require.NoError(t, newNode.Decode(&gen))

	return gen
}

func TestDocumentationCoverage(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		conf := config.New()
		tConf := reflect.TypeOf(conf)

		spec := docs.FieldCommon("", "").WithChildren(config.Spec()...)
		walkTypeWithConfig(t, "root", spec, tConf)
	})

	t.Run("buffers", func(t *testing.T) {
		conf := buffer.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllBuffers.Docs() {
			if v.Plugin {
				continue
			}
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeBuffer, conf)
			walkSpecWithConfig(t, "buffer."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "buffer."+v.Name, v.Config, cConf)
			}
		}
	})

	t.Run("caches", func(t *testing.T) {
		conf := cache.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllCaches.Docs() {
			if v.Plugin {
				continue
			}
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeCache, conf)
			walkSpecWithConfig(t, "cache."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "cache."+v.Name, v.Config, cConf)
			}
		}
	})

	t.Run("inputs", func(t *testing.T) {
		conf := input.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllInputs.Docs() {
			if v.Plugin {
				continue
			}
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeInput, conf)
			walkSpecWithConfig(t, "input."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "input."+v.Name, v.Config, cConf)
			}
		}
	})

	t.Run("metrics", func(t *testing.T) {
		conf := metrics.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllMetrics.Docs() {
			if v.Plugin {
				continue
			}
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeMetrics, conf)
			walkSpecWithConfig(t, "metrics."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "metrics."+v.Name, v.Config, cConf)
			}
		}
	})

	t.Run("outputs", func(t *testing.T) {
		conf := output.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllOutputs.Docs() {
			if v.Plugin {
				continue
			}
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeOutput, conf)
			walkSpecWithConfig(t, "outputs."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "output."+v.Name, v.Config, cConf)
			}
		}
	})

	t.Run("processors", func(t *testing.T) {
		conf := processor.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllProcessors.Docs() {
			if v.Plugin {
				continue
			}
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeProcessor, conf)
			walkSpecWithConfig(t, "processor."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "processor."+v.Name, v.Config, cConf)
			}
		}
	})

	t.Run("rate limits", func(t *testing.T) {
		conf := ratelimit.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllRateLimits.Docs() {
			if v.Plugin {
				continue
			}
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeRateLimit, conf)
			walkSpecWithConfig(t, "rate_limit."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "rate_limit."+v.Name, v.Config, cConf)
			}
		}
	})

	t.Run("tracers", func(t *testing.T) {
		conf := tracer.NewConfig()
		tConf := reflect.TypeOf(conf)
		for _, v := range bundle.AllTracers.Docs() {
			conf.Type = v.Name
			gen := getGenericConf(t, docs.TypeTracer, conf)
			walkSpecWithConfig(t, "tracer."+v.Name, v.Config, gen[v.Name])

			cConf, ok := getFieldByYAMLTag(tConf, v.Name)
			if v.Status != docs.StatusDeprecated && assert.True(t, ok, v.Name) {
				walkTypeWithConfig(t, "tracer."+v.Name, v.Config, cConf)
			}
		}
	})
}
