package template

import (
	"fmt"
	"io/fs"
	"sync"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	icache "github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	iratelimit "github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/template"
	"gopkg.in/yaml.v3"
)

var initNativeOnce sync.Once

func initNativeTemplates() {
	if werr := fs.WalkDir(template.NativeTemplates, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		tBytes, err := fs.ReadFile(template.NativeTemplates, path)
		if err != nil {
			return err
		}

		var conf Config
		if err = yaml.Unmarshal(tBytes, &conf); err != nil {
			return fmt.Errorf("failed to parse template '%v': %w", path, err)
		}

		tmpl, err := conf.compile()
		if err != nil {
			return fmt.Errorf("failed to compile template %v: %w", path, err)
		}

		if err := registerTemplate(tmpl); err != nil {
			return fmt.Errorf("failed to register template %v: %w", path, err)
		}

		return nil
	}); werr != nil {
		panic(werr)
	}
}

// InitTemplates parses and registers native templates, as well as templates
// at paths provided, and returns any linting errors that occur.
func InitTemplates(templatesPaths ...string) ([]string, error) {
	initNativeOnce.Do(initNativeTemplates)

	var lints []string
	for _, tPath := range templatesPaths {
		tmplConf, tLints, err := ReadConfig(tPath)
		if err != nil {
			return nil, fmt.Errorf("template %v: %w", tPath, err)
		}
		for _, l := range tLints {
			lints = append(lints, fmt.Sprintf("template file %v: %v", tPath, l))
		}

		tmpl, err := tmplConf.compile()
		if err != nil {
			return nil, fmt.Errorf("template %v: %w", tPath, err)
		}

		if err := registerTemplate(tmpl); err != nil {
			return nil, fmt.Errorf("template %v: %w", tPath, err)
		}
	}
	return lints, nil
}

//------------------------------------------------------------------------------

// Compiled is a template that has been compiled from a config.
type compiled struct {
	spec           docs.ComponentSpec
	mapping        *mapping.Executor
	metricsMapping *metrics.Mapping
}

// ExpandToNode attempts to apply the template to a provided YAML node and
// returns the new expanded configuration.
func (c *compiled) ExpandToNode(node *yaml.Node) (*yaml.Node, error) {
	generic, err := c.spec.Config.Children.YAMLToMap(node, docs.ToValueConfig{})
	if err != nil {
		return nil, fmt.Errorf("invalid config for template component: %w", err)
	}

	msg := message.QuickBatch(nil)
	part := message.NewPart(nil)
	if err := part.SetJSON(generic); err != nil {
		return nil, err
	}
	msg.Append(part)

	newPart, err := c.mapping.MapPart(0, msg)
	if err != nil {
		return nil, fmt.Errorf("mapping failed for template component: %w", err)
	}

	resultGeneric, err := newPart.JSON()
	if err != nil {
		return nil, fmt.Errorf("mapping for template component resulted in invalid config: %w", err)
	}

	var resultNode yaml.Node
	if err := resultNode.Encode(resultGeneric); err != nil {
		return nil, fmt.Errorf("mapping for template component resulted in invalid yaml: %w", err)
	}

	return &resultNode, nil
}

//------------------------------------------------------------------------------

// RegisterTemplate attempts to add a template component to the global list of
// component types.
func registerTemplate(tmpl *compiled) error {
	switch tmpl.spec.Type {
	case docs.TypeCache:
		return registerCacheTemplate(tmpl, bundle.AllCaches)
	case docs.TypeInput:
		return registerInputTemplate(tmpl, bundle.AllInputs)
	case docs.TypeOutput:
		return registerOutputTemplate(tmpl, bundle.AllOutputs)
	case docs.TypeProcessor:
		return registerProcessorTemplate(tmpl, bundle.AllProcessors)
	case docs.TypeRateLimit:
		return registerRateLimitTemplate(tmpl, bundle.AllRateLimits)
	}
	return fmt.Errorf("unable to register template for component type %v", tmpl.spec.Type)
}

// WithMetricsMapping attempts to wrap the metrics of a manager with a metrics
// mapping.
func WithMetricsMapping(nm bundle.NewManagement, m *metrics.Mapping) bundle.NewManagement {
	if t, ok := nm.(*manager.Type); ok {
		return t.WithMetricsMapping(m)
	}
	return nm
}

func registerCacheTemplate(tmpl *compiled, set *bundle.CacheSet) error {
	return set.Add(func(c cache.Config, nm bundle.NewManagement) (icache.V1, error) {
		newNode, err := tmpl.ExpandToNode(c.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}

		conf := cache.NewConfig()
		if err := newNode.Decode(&conf); err != nil {
			return nil, err
		}

		if tmpl.metricsMapping != nil {
			nm = WithMetricsMapping(nm, tmpl.metricsMapping)
		}
		return nm.NewCache(conf)
	}, tmpl.spec)
}

func registerInputTemplate(tmpl *compiled, set *bundle.InputSet) error {
	return set.Add(func(c input.Config, nm bundle.NewManagement, pcf ...types.PipelineConstructorFunc) (input.Type, error) {
		newNode, err := tmpl.ExpandToNode(c.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}

		conf := input.NewConfig()
		if err := newNode.Decode(&conf); err != nil {
			return nil, err
		}

		// Tempate processors inserted _before_ configured processors.
		conf.Processors = append(conf.Processors, c.Processors...)

		if tmpl.metricsMapping != nil {
			nm = WithMetricsMapping(nm, tmpl.metricsMapping)
		}
		return nm.NewInput(conf, pcf...)
	}, tmpl.spec)
}

func registerOutputTemplate(tmpl *compiled, set *bundle.OutputSet) error {
	return set.Add(func(c output.Config, nm bundle.NewManagement, pcf ...types.PipelineConstructorFunc) (output.Type, error) {
		newNode, err := tmpl.ExpandToNode(c.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}

		conf := output.NewConfig()
		if err := newNode.Decode(&conf); err != nil {
			return nil, err
		}

		// Tempate processors inserted _after_ configured processors.
		conf.Processors = append(c.Processors, conf.Processors...)

		if tmpl.metricsMapping != nil {
			nm = WithMetricsMapping(nm, tmpl.metricsMapping)
		}
		return nm.NewOutput(conf, pcf...)
	}, tmpl.spec)
}

func registerProcessorTemplate(tmpl *compiled, set *bundle.ProcessorSet) error {
	return set.Add(func(c processor.Config, nm bundle.NewManagement) (processor.Type, error) {
		newNode, err := tmpl.ExpandToNode(c.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}

		conf := processor.NewConfig()
		if err := newNode.Decode(&conf); err != nil {
			return nil, err
		}

		if tmpl.metricsMapping != nil {
			nm = WithMetricsMapping(nm, tmpl.metricsMapping)
		}
		return nm.NewProcessor(conf)
	}, tmpl.spec)
}

func registerRateLimitTemplate(tmpl *compiled, set *bundle.RateLimitSet) error {
	return set.Add(func(c ratelimit.Config, nm bundle.NewManagement) (iratelimit.V1, error) {
		newNode, err := tmpl.ExpandToNode(c.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}

		conf := ratelimit.NewConfig()
		if err := newNode.Decode(&conf); err != nil {
			return nil, err
		}

		if tmpl.metricsMapping != nil {
			nm = WithMetricsMapping(nm, tmpl.metricsMapping)
		}
		return nm.NewRateLimit(conf)
	}, tmpl.spec)
}
