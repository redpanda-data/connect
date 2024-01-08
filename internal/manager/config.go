package manager

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/docs"
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

// FromYAML is for old style tests.
func FromYAML(confStr string) (conf ResourceConfig, err error) {
	var node yaml.Node
	if err = yaml.Unmarshal([]byte(confStr), &node); err != nil {
		return
	}
	conf = NewResourceConfig()
	err = conf.fromYAML(docs.DeprecatedProvider, node.Content[0])
	return
}

func (r *ResourceConfig) FromAny(prov docs.Provider, value any) (err error) {
	switch t := value.(type) {
	case ResourceConfig:
		*r = t
		return
	case *yaml.Node:
		return r.fromYAML(prov, t)
	case map[string]any:
		return r.fromMap(prov, t)
	}
	err = fmt.Errorf("unexpected value, expected object, got %T", value)
	return
}

func (r *ResourceConfig) fromMap(prov docs.Provider, value map[string]any) (err error) {
	if cList, ok := value["input_resources"].([]any); ok {
		for i, iv := range cList {
			var iConf input.Config
			if iConf, err = input.FromAny(prov, iv); err != nil {
				err = fmt.Errorf("resource %v: %w", i, err)
				return
			}
			r.ResourceInputs = append(r.ResourceInputs, iConf)
		}
	}
	if cList, ok := value["processor_resources"].([]any); ok {
		for i, iv := range cList {
			var iConf processor.Config
			if iConf, err = processor.FromAny(prov, iv); err != nil {
				err = fmt.Errorf("resource %v: %w", i, err)
				return
			}
			r.ResourceProcessors = append(r.ResourceProcessors, iConf)
		}
	}
	if cList, ok := value["output_resources"].([]any); ok {
		for i, iv := range cList {
			var iConf output.Config
			if iConf, err = output.FromAny(prov, iv); err != nil {
				err = fmt.Errorf("resource %v: %w", i, err)
				return
			}
			r.ResourceOutputs = append(r.ResourceOutputs, iConf)
		}
	}
	if cList, ok := value["cache_resources"].([]any); ok {
		for i, iv := range cList {
			var iConf cache.Config
			if iConf, err = cache.FromAny(prov, iv); err != nil {
				err = fmt.Errorf("resource %v: %w", i, err)
				return
			}
			r.ResourceCaches = append(r.ResourceCaches, iConf)
		}
	}
	if cList, ok := value["rate_limit_resources"].([]any); ok {
		for i, iv := range cList {
			var iConf ratelimit.Config
			if iConf, err = ratelimit.FromAny(prov, iv); err != nil {
				err = fmt.Errorf("resource %v: %w", i, err)
				return
			}
			r.ResourceRateLimits = append(r.ResourceRateLimits, iConf)
		}
	}
	return
}

func (r *ResourceConfig) fromYAML(prov docs.Provider, value *yaml.Node) (err error) {
	for i := 0; i < len(value.Content)-1; i += 2 {
		key := value.Content[i].Value
		children := value.Content[i+1].Content
		switch key {
		case "input_resources":
			for _, c := range children {
				var iConf input.Config
				if iConf, err = input.FromAny(prov, c); err != nil {
					return
				}
				r.ResourceInputs = append(r.ResourceInputs, iConf)
			}
		case "processor_resources":
			for _, c := range children {
				var iConf processor.Config
				if iConf, err = processor.FromAny(prov, c); err != nil {
					return
				}
				r.ResourceProcessors = append(r.ResourceProcessors, iConf)
			}
		case "output_resources":
			for _, c := range children {
				var iConf output.Config
				if iConf, err = output.FromAny(prov, c); err != nil {
					return
				}
				r.ResourceOutputs = append(r.ResourceOutputs, iConf)
			}
		case "cache_resources":
			for _, c := range children {
				var iConf cache.Config
				if iConf, err = cache.FromAny(prov, c); err != nil {
					return
				}
				r.ResourceCaches = append(r.ResourceCaches, iConf)
			}
		case "rate_limit_resources":
			for _, c := range children {
				var iConf ratelimit.Config
				if iConf, err = ratelimit.FromAny(prov, c); err != nil {
					return
				}
				r.ResourceRateLimits = append(r.ResourceRateLimits, iConf)
			}
		}
	}
	return
}
