package manager

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/util/config"
)

type ResourceConfig struct {
	// Called manager for backwards compatibility.
	Manager            Config             `json:"resources,omitempty" yaml:"resources,omitempty"`
	ResourceInputs     []input.Config     `json:"input_resources,omitempty" yaml:"input_resources,omitempty"`
	ResourceProcessors []processor.Config `json:"processor_resources,omitempty" yaml:"processor_resources,omitempty"`
	ResourceOutputs    []output.Config    `json:"output_resources,omitempty" yaml:"output_resources,omitempty"`
	ResourceCaches     []cache.Config     `json:"cache_resources,omitempty" yaml:"cache_resources,omitempty"`
	ResourceRateLimits []ratelimit.Config `json:"rate_limit_resources,omitempty" yaml:"rate_limit_resources,omitempty"`
}

func NewResourceConfig() ResourceConfig {
	return ResourceConfig{
		Manager:            NewConfig(),
		ResourceInputs:     []input.Config{},
		ResourceProcessors: []processor.Config{},
		ResourceOutputs:    []output.Config{},
		ResourceCaches:     []cache.Config{},
		ResourceRateLimits: []ratelimit.Config{},
	}
}

// Collapses all the slice based resources into maps, returning an error if any
// labels are duplicated or empty.
func (r *ResourceConfig) collapsed() (ResourceConfig, error) {
	newMaps := NewConfig()

	for k, v := range r.Manager.Caches {
		newMaps.Caches[k] = v
	}
	for _, c := range r.ResourceCaches {
		if c.Label == "" {
			return *r, errors.New("cache resource has an empty label")
		}
		if _, exists := newMaps.Caches[c.Label]; exists {
			return *r, fmt.Errorf("cache resource label '%v' collides with a previously defined resource", c.Label)
		}
		newMaps.Caches[c.Label] = c
	}

	for k, v := range r.Manager.Conditions {
		newMaps.Conditions[k] = v
	}

	for k, v := range r.Manager.Inputs {
		newMaps.Inputs[k] = v
	}
	for _, c := range r.ResourceInputs {
		if c.Label == "" {
			return *r, errors.New("input resource has an empty label")
		}
		if _, exists := newMaps.Inputs[c.Label]; exists {
			return *r, fmt.Errorf("input resource label '%v' collides with a previously defined resource", c.Label)
		}
		newMaps.Inputs[c.Label] = c
	}

	for k, v := range r.Manager.Outputs {
		newMaps.Outputs[k] = v
	}
	for _, c := range r.ResourceOutputs {
		if c.Label == "" {
			return *r, errors.New("output resource has an empty label")
		}
		if _, exists := newMaps.Outputs[c.Label]; exists {
			return *r, fmt.Errorf("output resource label '%v' collides with a previously defined resource", c.Label)
		}
		newMaps.Outputs[c.Label] = c
	}

	for k, v := range r.Manager.Plugins {
		newMaps.Plugins[k] = v
	}

	for k, v := range r.Manager.Processors {
		newMaps.Processors[k] = v
	}
	for _, c := range r.ResourceProcessors {
		if c.Label == "" {
			return *r, errors.New("processor resource has an empty label")
		}
		if _, exists := newMaps.Processors[c.Label]; exists {
			return *r, fmt.Errorf("processor resource label '%v' collides with a previously defined resource", c.Label)
		}
		newMaps.Processors[c.Label] = c
	}

	for k, v := range r.Manager.RateLimits {
		newMaps.RateLimits[k] = v
	}
	for _, c := range r.ResourceRateLimits {
		if c.Label == "" {
			return *r, errors.New("rate limit resource has an empty label")
		}
		if _, exists := newMaps.RateLimits[c.Label]; exists {
			return *r, fmt.Errorf("rate limit resource label '%v' collides with a previously defined resource", c.Label)
		}
		newMaps.RateLimits[c.Label] = c
	}

	return ResourceConfig{
		Manager: newMaps,
	}, nil
}

// AddFrom takes another Config and adds all of its resources to itself. If
// there are any resource name collisions an error is returned.
func (r *ResourceConfig) AddFrom(extra *ResourceConfig) error {
	if err := r.Manager.AddFrom(&extra.Manager); err != nil {
		return err
	}
	// TODO: Detect duplicates.
	r.ResourceInputs = append(r.ResourceInputs, extra.ResourceInputs...)
	r.ResourceProcessors = append(r.ResourceProcessors, extra.ResourceProcessors...)
	r.ResourceOutputs = append(r.ResourceOutputs, extra.ResourceOutputs...)
	r.ResourceCaches = append(r.ResourceCaches, extra.ResourceCaches...)
	r.ResourceRateLimits = append(r.ResourceRateLimits, extra.ResourceRateLimits...)
	return nil
}

// Config contains all configuration fields for a Benthos service manager.
type Config struct {
	Inputs     map[string]input.Config     `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Conditions map[string]condition.Config `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	Processors map[string]processor.Config `json:"processors,omitempty" yaml:"processors,omitempty"`
	Outputs    map[string]output.Config    `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Caches     map[string]cache.Config     `json:"caches,omitempty" yaml:"caches,omitempty"`
	RateLimits map[string]ratelimit.Config `json:"rate_limits,omitempty" yaml:"rate_limits,omitempty"`
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
