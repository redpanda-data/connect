package service_test

import (
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/public/x/service"
	"github.com/stretchr/testify/assert"
)

func walkForSummaries(fn func(func(name string, config *service.ConfigView))) map[string]string {
	summaries := map[string]string{}
	fn(func(name string, config *service.ConfigView) {
		summaries[name] = config.Summary()
	})
	return summaries
}

func TestEnvironmentAdjustments(t *testing.T) {
	envOne := service.NewEnvironment()
	envTwo := envOne.Clone()

	assert.NoError(t, envOne.RegisterCache(
		"one_cache", service.NewConfigSpec().Summary("cache one"),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return nil, errors.New("cache one err")
		},
	))
	assert.NoError(t, envOne.RegisterInput(
		"one_input", service.NewConfigSpec().Summary("input one"),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return nil, errors.New("input one err")
		},
	))
	assert.NoError(t, envOne.RegisterOutput(
		"one_output", service.NewConfigSpec().Summary("output one"),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			return nil, 0, errors.New("output one err")
		},
	))
	assert.NoError(t, envOne.RegisterProcessor(
		"one_processor", service.NewConfigSpec().Summary("processor one"),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return nil, errors.New("processor one err")
		},
	))
	assert.NoError(t, envOne.RegisterRateLimit(
		"one_rate_limit", service.NewConfigSpec().Summary("rate limit one"),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			return nil, errors.New("rate limit one err")
		},
	))

	assert.Equal(t, "cache one", walkForSummaries(envOne.WalkCaches)["one_cache"])
	assert.Equal(t, "input one", walkForSummaries(envOne.WalkInputs)["one_input"])
	assert.Equal(t, "output one", walkForSummaries(envOne.WalkOutputs)["one_output"])
	assert.Equal(t, "processor one", walkForSummaries(envOne.WalkProcessors)["one_processor"])
	assert.Equal(t, "rate limit one", walkForSummaries(envOne.WalkRateLimits)["one_rate_limit"])

	assert.NotContains(t, walkForSummaries(envTwo.WalkCaches), "one_cache")
	assert.NotContains(t, walkForSummaries(envTwo.WalkInputs), "one_input")
	assert.NotContains(t, walkForSummaries(envTwo.WalkOutputs), "one_output")
	assert.NotContains(t, walkForSummaries(envTwo.WalkProcessors), "one_processor")
	assert.NotContains(t, walkForSummaries(envTwo.WalkRateLimits), "one_rate_limit")

	testConfig := `
input:
  one_input: {}
pipeline:
  processors:
    - one_processor: {}
output:
  one_output: {}
cache_resources:
  - label: foocache
    one_cache: {}
rate_limit_resources:
  - label: foorl
    one_rate_limit: {}
`

	assert.NoError(t, envOne.NewStreamBuilder().SetYAML(testConfig))
	assert.Error(t, envTwo.NewStreamBuilder().SetYAML(testConfig))
}
