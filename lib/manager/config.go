package manager

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/util/config"
)

// Config contains all configuration fields for a Benthos service manager.
type Config struct {
	Inputs     map[string]input.Config     `json:"inputs" yaml:"inputs"`
	Conditions map[string]condition.Config `json:"conditions" yaml:"conditions"`
	Processors map[string]processor.Config `json:"processors" yaml:"processors"`
	Outputs    map[string]output.Config    `json:"outputs" yaml:"outputs"`
	Caches     map[string]cache.Config     `json:"caches" yaml:"caches"`
	RateLimits map[string]ratelimit.Config `json:"rate_limits" yaml:"rate_limits"`
	Plugins    map[string]PluginConfig     `json:"plugins,omitempty" yaml:"plugins,omitempty"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		Inputs:     map[string]input.Config{},
		Conditions: map[string]condition.Config{},
		Processors: map[string]processor.Config{},
		Outputs:    map[string]output.Config{},
		Caches:     map[string]cache.Config{},
		RateLimits: map[string]ratelimit.Config{},
		Plugins:    map[string]PluginConfig{},
	}
}

// AddFrom takes another Config and adds all of its resources to itself. If
// there are any resource name collisions an error is returned.
func (c *Config) AddFrom(extra *Config) error {
	for k, v := range extra.Inputs {
		if _, exists := c.Inputs[k]; exists {
			return fmt.Errorf("resource input name collision: %v", k)
		}
		c.Inputs[k] = v
	}
	for k, v := range extra.Conditions {
		if _, exists := c.Conditions[k]; exists {
			return fmt.Errorf("resource condition name collision: %v", k)
		}
		c.Conditions[k] = v
	}
	for k, v := range extra.Processors {
		if _, exists := c.Processors[k]; exists {
			return fmt.Errorf("resource processor name collision: %v", k)
		}
		c.Processors[k] = v
	}
	for k, v := range extra.Outputs {
		if _, exists := c.Outputs[k]; exists {
			return fmt.Errorf("resource output name collision: %v", k)
		}
		c.Outputs[k] = v
	}
	for k, v := range extra.Caches {
		if _, exists := c.Caches[k]; exists {
			return fmt.Errorf("resource cache name collision: %v", k)
		}
		c.Caches[k] = v
	}
	for k, v := range extra.RateLimits {
		if _, exists := c.RateLimits[k]; exists {
			return fmt.Errorf("resource ratelimit name collision: %v", k)
		}
		c.RateLimits[k] = v
	}
	for k, v := range extra.Plugins {
		if _, exists := c.Plugins[k]; exists {
			return fmt.Errorf("resource plugin name collision: %v", k)
		}
		c.Plugins[k] = v
	}
	return nil
}

// AddExamples inserts example caches and conditions if none exist in the
// config.
func AddExamples(c *Config) {
	if len(c.Inputs) == 0 {
		c.Inputs["example"] = input.NewConfig()
	}
	if len(c.Conditions) == 0 {
		c.Conditions["example"] = condition.NewConfig()
	}
	if len(c.Processors) == 0 {
		c.Processors["example"] = processor.NewConfig()
	}
	if len(c.Outputs) == 0 {
		c.Outputs["example"] = output.NewConfig()
	}
	if len(c.Caches) == 0 {
		c.Caches["example"] = cache.NewConfig()
	}
	if len(c.RateLimits) == 0 {
		c.RateLimits["example"] = ratelimit.NewConfig()
	}
}

//------------------------------------------------------------------------------

// SanitiseConfig creates a sanitised version of a manager config.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (c Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	var err error

	inputs := map[string]interface{}{}
	for k, v := range c.Inputs {
		if inputs[k], err = v.Sanitised(removeDeprecated); err != nil {
			return nil, err
		}
	}

	caches := map[string]interface{}{}
	for k, v := range c.Caches {
		if caches[k], err = v.Sanitised(removeDeprecated); err != nil {
			return nil, err
		}
	}

	conditions := map[string]interface{}{}
	for k, v := range c.Conditions {
		if conditions[k], err = v.Sanitised(removeDeprecated); err != nil {
			return nil, err
		}
	}

	processors := map[string]interface{}{}
	for k, v := range c.Processors {
		if processors[k], err = v.Sanitised(removeDeprecated); err != nil {
			return nil, err
		}
	}

	outputs := map[string]interface{}{}
	for k, v := range c.Outputs {
		if outputs[k], err = v.Sanitised(removeDeprecated); err != nil {
			return nil, err
		}
	}

	rateLimits := map[string]interface{}{}
	for k, v := range c.RateLimits {
		if rateLimits[k], err = v.Sanitised(removeDeprecated); err != nil {
			return nil, err
		}
	}

	plugins := map[string]interface{}{}
	for k, v := range c.Plugins {
		if spec, exists := pluginSpecs[v.Type]; exists {
			if spec.confSanitiser != nil {
				outputMap := config.Sanitised{}
				outputMap["type"] = v.Type
				outputMap["plugin"] = spec.confSanitiser(v.Plugin)
				plugins[k] = outputMap
			} else {
				plugins[k] = v
			}
		}
	}

	m := map[string]interface{}{
		"inputs":      inputs,
		"conditions":  conditions,
		"processors":  processors,
		"outputs":     outputs,
		"caches":      caches,
		"rate_limits": rateLimits,
	}
	if len(plugins) > 0 {
		m["plugins"] = plugins
	}
	return m, nil
}
