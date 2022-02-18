package service_test

import (
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestCachePluginWithConfig(t *testing.T) {
	type testConfig struct {
		A int `yaml:"a"`
	}

	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &testConfig{A: 100}
	})
	require.NoError(t, err)

	var initConf *testConfig
	var initLabel string
	require.NoError(t, service.RegisterCache("test_cache_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			initConf = conf.AsStruct().(*testConfig)
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	cacheConfStr := `label: foo
test_cache_plugin_with_config:
    a: 20
`

	cacheConf := cache.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(cacheConfStr), &cacheConf))

	var cacheNode yaml.Node
	require.NoError(t, cacheNode.Encode(cacheConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeCache, &cacheNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	cacheConfOutBytes, err := yaml.Marshal(cacheNode)
	require.NoError(t, err)
	assert.Equal(t, cacheConfStr, string(cacheConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewCache(cacheConf)
	assert.EqualError(t, err, "this is a test error")
	require.NotNil(t, initConf)
	assert.Equal(t, 20, initConf.A)
	assert.Equal(t, "foo", initLabel)
}

func TestCachePluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterCache("test_cache_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	cacheConfStr := `label: foo
test_cache_plugin_without_config: null
`

	cacheConf := cache.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(cacheConfStr), &cacheConf))

	var cacheNode yaml.Node
	require.NoError(t, cacheNode.Encode(cacheConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeCache, &cacheNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	cacheConfOutBytes, err := yaml.Marshal(cacheNode)
	require.NoError(t, err)
	assert.Equal(t, cacheConfStr, string(cacheConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewCache(cacheConf)
	assert.EqualError(t, err, "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestInputPluginWithConfig(t *testing.T) {
	type testConfig struct {
		A int `yaml:"a"`
	}

	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &testConfig{A: 100}
	})
	require.NoError(t, err)

	var initConf *testConfig
	var initLabel string
	require.NoError(t, service.RegisterInput("test_input_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			initConf = conf.AsStruct().(*testConfig)
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_input_plugin_with_config:
    a: 20
`

	inConf := input.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeInput, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewInput(inConf)
	assert.EqualError(t, err, "failed to create input 'test_input_plugin_with_config': this is a test error")
	require.NotNil(t, initConf)
	assert.Equal(t, 20, initConf.A)
	assert.Equal(t, "foo", initLabel)
}

func TestInputPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterInput("test_input_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_input_plugin_without_config: null
`

	inConf := input.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeInput, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewInput(inConf)
	assert.EqualError(t, err, "failed to create input 'test_input_plugin_without_config': this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestOutputPluginWithConfig(t *testing.T) {
	type testConfig struct {
		A int `yaml:"a"`
	}

	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &testConfig{A: 100}
	})
	require.NoError(t, err)

	var initConf *testConfig
	var initLabel string
	require.NoError(t, service.RegisterOutput("test_output_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			initConf = conf.AsStruct().(*testConfig)
			initLabel = mgr.Label()
			return nil, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_output_plugin_with_config:
    a: 20
`

	inConf := output.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	assert.EqualError(t, err, "failed to create output 'test_output_plugin_with_config': this is a test error")
	require.NotNil(t, initConf)
	assert.Equal(t, 20, initConf.A)
	assert.Equal(t, "foo", initLabel)
}

func TestOutputPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterOutput("test_output_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			initLabel = mgr.Label()
			return nil, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_output_plugin_without_config: null
`

	inConf := output.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	assert.EqualError(t, err, "failed to create output 'test_output_plugin_without_config': this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestBatchOutputPluginWithConfig(t *testing.T) {
	type testConfig struct {
		A     int `yaml:"a"`
		Count int `yaml:"count"`
	}

	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &testConfig{A: 100, Count: 10}
	})
	require.NoError(t, err)

	var initConf *testConfig
	var initLabel string
	require.NoError(t, service.RegisterBatchOutput("test_batch_output_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			initConf = conf.AsStruct().(*testConfig)
			initLabel = mgr.Label()
			batchPolicy := service.BatchPolicy{Count: initConf.Count}
			return nil, batchPolicy, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_output_plugin_with_config:
    a: 20
    count: 21
`

	inConf := output.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	assert.EqualError(t, err, "failed to create output 'test_batch_output_plugin_with_config': this is a test error")
	require.NotNil(t, initConf)
	assert.Equal(t, 20, initConf.A)
	assert.Equal(t, 21, initConf.Count)
	assert.Equal(t, "foo", initLabel)
}

func TestBatchOutputPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterOutput("test_batch_output_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			initLabel = mgr.Label()
			return nil, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_output_plugin_without_config: null
`

	inConf := output.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	assert.EqualError(t, err, "failed to create output 'test_batch_output_plugin_without_config': this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestProcessorPluginWithConfig(t *testing.T) {
	type testConfig struct {
		A int `yaml:"a"`
	}

	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &testConfig{A: 100}
	})
	require.NoError(t, err)

	var initConf *testConfig
	var initLabel string
	require.NoError(t, service.RegisterProcessor("test_processor_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			initConf = conf.AsStruct().(*testConfig)
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_processor_plugin_with_config:
    a: 20
`

	inConf := processor.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	assert.EqualError(t, err, "this is a test error")
	require.NotNil(t, initConf)
	assert.Equal(t, 20, initConf.A)
	assert.Equal(t, "foo", initLabel)
}

func TestProcessorPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterProcessor("test_processor_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_processor_plugin_without_config: null
`

	inConf := processor.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	assert.EqualError(t, err, "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestBatchProcessorPluginWithConfig(t *testing.T) {
	type testConfig struct {
		A int `yaml:"a"`
	}

	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &testConfig{A: 100}
	})
	require.NoError(t, err)

	var initConf *testConfig
	var initLabel string
	require.NoError(t, service.RegisterBatchProcessor("test_batch_processor_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			initConf = conf.AsStruct().(*testConfig)
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_processor_plugin_with_config:
    a: 20
`

	inConf := processor.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	assert.EqualError(t, err, "this is a test error")
	require.NotNil(t, initConf)
	assert.Equal(t, 20, initConf.A)
	assert.Equal(t, "foo", initLabel)
}

func TestBatchProcessorPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterBatchProcessor("test_batch_processor_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_processor_plugin_without_config: null
`

	inConf := processor.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	assert.EqualError(t, err, "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestRateLimitPluginWithConfig(t *testing.T) {
	type testConfig struct {
		A int `yaml:"a"`
	}

	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &testConfig{A: 100}
	})
	require.NoError(t, err)

	var initConf *testConfig
	var initLabel string
	require.NoError(t, service.RegisterRateLimit("test_rate_limit_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			initConf = conf.AsStruct().(*testConfig)
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_rate_limit_plugin_with_config:
    a: 20
`

	inConf := ratelimit.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeRateLimit, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewRateLimit(inConf)
	assert.EqualError(t, err, "this is a test error")
	require.NotNil(t, initConf)
	assert.Equal(t, 20, initConf.A)
	assert.Equal(t, "foo", initLabel)
}

func TestRateLimitPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterRateLimit("test_rate_limit_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_rate_limit_plugin_without_config: null
`

	inConf := ratelimit.NewConfig()
	require.NoError(t, yaml.Unmarshal([]byte(inConfStr), &inConf))

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeRateLimit, &outNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: true,
	}))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.NewV2(manager.NewResourceConfig(), mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	_, err = mgr.NewRateLimit(inConf)
	assert.EqualError(t, err, "this is a test error")
	assert.Equal(t, "foo", initLabel)
}
