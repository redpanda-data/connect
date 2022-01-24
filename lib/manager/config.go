package manager

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
)

// ResourceConfig contains fields for specifying resource components at the root
// of a Benthos config.
type ResourceConfig struct {
	// Called manager for backwards compatibility.
	Manager            Config             `json:"resources,omitempty" yaml:"resources,omitempty"`
	ResourceInputs     []input.Config     `json:"input_resources,omitempty" yaml:"input_resources,omitempty"`
	ResourceProcessors []processor.Config `json:"processor_resources,omitempty" yaml:"processor_resources,omitempty"`
	ResourceOutputs    []output.Config    `json:"output_resources,omitempty" yaml:"output_resources,omitempty"`
	ResourceCaches     []cache.Config     `json:"cache_resources,omitempty" yaml:"cache_resources,omitempty"`
	ResourceRateLimits []ratelimit.Config `json:"rate_limit_resources,omitempty" yaml:"rate_limit_resources,omitempty"`
}

// NewResourceConfig creates a ResourceConfig with default values.
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
	Processors map[string]processor.Config `json:"processors,omitempty" yaml:"processors,omitempty"`
	Outputs    map[string]output.Config    `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Caches     map[string]cache.Config     `json:"caches,omitempty" yaml:"caches,omitempty"`
	RateLimits map[string]ratelimit.Config `json:"rate_limits,omitempty" yaml:"rate_limits,omitempty"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		Inputs:     map[string]input.Config{},
		Processors: map[string]processor.Config{},
		Outputs:    map[string]output.Config{},
		Caches:     map[string]cache.Config{},
		RateLimits: map[string]ratelimit.Config{},
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
	return nil
}

// AddExamples inserts example caches and conditions if none exist in the
// config.
func AddExamples(c *Config) {
	if len(c.Inputs) == 0 {
		c.Inputs["example"] = input.NewConfig()
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
