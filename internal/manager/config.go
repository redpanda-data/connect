package manager

import (
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
)

// ResourceConfig contains fields for specifying resource components at the root
// of a Benthos config.
type ResourceConfig struct {
	ResourceInputs     []input.Config     `json:"input_resources,omitempty" yaml:"input_resources,omitempty"`
	ResourceProcessors []processor.Config `json:"processor_resources,omitempty" yaml:"processor_resources,omitempty"`
	ResourceOutputs    []output.Config    `json:"output_resources,omitempty" yaml:"output_resources,omitempty"`
	ResourceCaches     []cache.Config     `json:"cache_resources,omitempty" yaml:"cache_resources,omitempty"`
	ResourceRateLimits []ratelimit.Config `json:"rate_limit_resources,omitempty" yaml:"rate_limit_resources,omitempty"`
}

// NewResourceConfig creates a ResourceConfig with default values.
func NewResourceConfig() ResourceConfig {
	return ResourceConfig{
		ResourceInputs:     []input.Config{},
		ResourceProcessors: []processor.Config{},
		ResourceOutputs:    []output.Config{},
		ResourceCaches:     []cache.Config{},
		ResourceRateLimits: []ratelimit.Config{},
	}
}

// AddFrom takes another Config and adds all of its resources to itself. If
// there are any resource name collisions an error is returned.
func (r *ResourceConfig) AddFrom(extra *ResourceConfig) error {
	r.ResourceInputs = append(r.ResourceInputs, extra.ResourceInputs...)
	r.ResourceProcessors = append(r.ResourceProcessors, extra.ResourceProcessors...)
	r.ResourceOutputs = append(r.ResourceOutputs, extra.ResourceOutputs...)
	r.ResourceCaches = append(r.ResourceCaches, extra.ResourceCaches...)
	r.ResourceRateLimits = append(r.ResourceRateLimits, extra.ResourceRateLimits...)
	return nil
}
