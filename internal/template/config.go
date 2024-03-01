package template

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fatih/color"
	"github.com/nsf/jsondiff"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
)

// FieldConfig describes a configuration field used in the template.
type FieldConfig struct {
	Name        string  `yaml:"name"`
	Description string  `yaml:"description"`
	Type        *string `yaml:"type,omitempty"`
	Kind        *string `yaml:"kind,omitempty"`
	Default     *any    `yaml:"default,omitempty"`
	Advanced    bool    `yaml:"advanced"`
}

// TestConfig defines a unit test for the template.
type TestConfig struct {
	Name     string    `yaml:"name"`
	Config   yaml.Node `yaml:"config"`
	Expected yaml.Node `yaml:"expected,omitempty"`
}

// Config describes a Benthos component template.
type Config struct {
	Name           string        `yaml:"name"`
	Type           string        `yaml:"type"`
	Status         string        `yaml:"status"`
	Categories     []string      `yaml:"categories"`
	Summary        string        `yaml:"summary"`
	Description    string        `yaml:"description"`
	Fields         []FieldConfig `yaml:"fields"`
	Mapping        string        `yaml:"mapping"`
	MetricsMapping string        `yaml:"metrics_mapping"`
	Tests          []TestConfig  `yaml:"tests"`
}

// FieldSpec creates a documentation field spec from a template field config.
func (c FieldConfig) FieldSpec() (docs.FieldSpec, error) {
	f := docs.FieldAnything(c.Name, c.Description)
	f.IsAdvanced = c.Advanced
	if c.Default != nil {
		f = f.HasDefault(*c.Default)
	}
	if c.Type == nil {
		return f, errors.New("missing type field")
	}
	f = f.HasType(docs.FieldType(*c.Type))
	if c.Kind != nil {
		switch *c.Kind {
		case "map":
			f = f.Map()
		case "list":
			f = f.Array()
		case "scalar":
		default:
			return f, fmt.Errorf("unrecognised scalar type: %v", *c.Kind)
		}
	}
	return f, nil
}

// ComponentSpec creates a documentation component spec from a template config.
func (c Config) ComponentSpec() (docs.ComponentSpec, error) {
	fields := make([]docs.FieldSpec, len(c.Fields))
	for i, fieldConf := range c.Fields {
		var err error
		if fields[i], err = fieldConf.FieldSpec(); err != nil {
			return docs.ComponentSpec{}, fmt.Errorf("field %v: %w", i, err)
		}
	}
	config := docs.FieldComponent().WithChildren(fields...)

	status := docs.StatusStable
	if c.Status != "" {
		status = docs.Status(c.Status)
	}

	return docs.ComponentSpec{
		Name:        c.Name,
		Type:        docs.Type(c.Type),
		Status:      status,
		Plugin:      true,
		Categories:  c.Categories,
		Summary:     c.Summary,
		Description: c.Description,
		Config:      config,
	}, nil
}

func (c Config) compile() (*compiled, error) {
	spec, err := c.ComponentSpec()
	if err != nil {
		return nil, err
	}
	mapping, err := bloblang.GlobalEnvironment().NewMapping(c.Mapping)
	if err != nil {
		var perr *parser.Error
		if errors.As(err, &perr) {
			return nil, fmt.Errorf("parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(c.Mapping)))
		}
		return nil, fmt.Errorf("parse mapping: %w", err)
	}
	var metricsMapping *metrics.Mapping
	if c.MetricsMapping != "" {
		if metricsMapping, err = metrics.NewMapping(c.MetricsMapping, log.Noop()); err != nil {
			return nil, fmt.Errorf("parse metrics mapping: %w", err)
		}
	}
	return &compiled{spec: spec, mapping: mapping, metricsMapping: metricsMapping}, nil
}

func diffYAMLNodesAsJSON(expNode *yaml.Node, actNode any) (string, error) {
	var iexp any
	if err := expNode.Decode(&iexp); err != nil {
		return "", fmt.Errorf("failed to marshal expected %w", err)
	}

	expBytes, err := json.Marshal(iexp)
	if err != nil {
		return "", fmt.Errorf("failed to marshal expected %w", err)
	}
	actBytes, err := json.Marshal(actNode)
	if err != nil {
		return "", fmt.Errorf("failed to marshal actual %w", err)
	}

	jdopts := jsondiff.DefaultConsoleOptions()
	diff, explanation := jsondiff.Compare(expBytes, actBytes, &jdopts)
	if diff != jsondiff.FullMatch {
		return explanation, nil
	}
	return "", nil
}

// Test ensures that the template compiles, and executes any unit test
// definitions within the config.
func (c Config) Test() ([]string, error) {
	compiled, err := c.compile()
	if err != nil {
		return nil, err
	}

	var failures []string
	for _, test := range c.Tests {
		outConf, err := compiled.Render(&test.Config)
		if err != nil {
			return nil, fmt.Errorf("test '%v': %w", test.Name, err)
		}

		var yNode yaml.Node
		if err := yNode.Encode(outConf); err == nil {
			for _, lint := range docs.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), docs.Type(c.Type), &yNode) {
				failures = append(failures, fmt.Sprintf("test '%v': lint error in resulting config: %v", test.Name, lint.Error()))
			}
		} else {
			failures = append(failures, fmt.Sprintf("test '%v': failed to encode resulting config as YAML: %v", test.Name, err.Error()))
		}
		if len(test.Expected.Content) > 0 {
			diff, err := diffYAMLNodesAsJSON(&test.Expected, outConf)
			if err != nil {
				return nil, fmt.Errorf("test '%v': %w", test.Name, err)
			}
			if diff != "" {
				diff = color.New(color.Reset).SprintFunc()(diff)
				return nil, fmt.Errorf("test '%v': mismatch between expected and actual resulting config: %v", test.Name, diff)
			}
		}
	}
	return failures, nil
}

// ReadConfigYAML attempts to read a YAML byte slice as a template configuration
// file.
func ReadConfigYAML(templateBytes []byte) (conf Config, lints []docs.Lint, err error) {
	if err = yaml.Unmarshal(templateBytes, &conf); err != nil {
		return
	}

	var node yaml.Node
	if err = yaml.Unmarshal(templateBytes, &node); err != nil {
		return
	}

	lints = ConfigSpec().LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), &node)
	return
}

// ReadConfigFile attempts to read a template configuration file.
func ReadConfigFile(path string) (conf Config, lints []docs.Lint, err error) {
	var templateBytes []byte
	if templateBytes, err = ifs.ReadFile(ifs.OS(), path); err != nil {
		return
	}
	return ReadConfigYAML(templateBytes)
}

//------------------------------------------------------------------------------

// FieldConfigSpec returns a configuration spec for a field of a template.
func FieldConfigSpec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("name", "The name of the field."),
		docs.FieldString("description", "A description of the field.").HasDefault(""),
		docs.FieldString("type", "The scalar type of the field.").HasAnnotatedOptions(
			"string", "standard string type",
			"int", "standard integer type",
			"float", "standard float type",
			"bool", "a boolean true/false",
			"unknown", "allows for nesting arbitrary configuration inside of a field",
		),
		docs.FieldString("kind", "The kind of the field.").HasOptions(
			"scalar", "map", "list",
		).HasDefault("scalar"),
		docs.FieldAnything("default", "An optional default value for the field. If a default value is not specified then a configuration without the field is considered incorrect.").Optional(),
		docs.FieldBool("advanced", "Whether this field is considered advanced.").HasDefault(false),
	}
}

func templateMetricsMappingDocs() docs.FieldSpec {
	f := docs.MetricsMappingFieldSpec("metrics_mapping")
	f.Description += `

Invocations of this mapping are able to reference a variable $label in order to obtain the value of the label provided to the template config. This allows you to match labels with the root of the config.`
	return f
}

// ConfigSpec returns a configuration spec for a template.
func ConfigSpec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("name", "The name of the component this template will create."),
		docs.FieldString(
			"type", "The type of the component this template will create.",
		).HasOptions(
			"cache", "input", "output", "processor", "rate_limit",
		),
		docs.FieldString(
			"status", "The stability of the template describing the likelihood that the configuration spec of the template, or it's behaviour, will change.",
		).HasAnnotatedOptions(
			"stable", "This template is stable and will therefore not change in a breaking way outside of major version releases.",
			"beta", "This template is beta and will therefore not change in a breaking way unless a major problem is found.",
			"experimental", "This template is experimental and therefore subject to breaking changes outside of major version releases.",
		).HasDefault("stable"),
		docs.FieldString(
			"categories", "An optional list of tags, which are used for arbitrarily grouping components in documentation.",
		).Array().HasDefault([]any{}),
		docs.FieldString("summary", "A short summary of the component.").HasDefault(""),
		docs.FieldString("description", "A longer form description of the component and how to use it.").HasDefault(""),
		docs.FieldObject("fields", "The configuration fields of the template, fields specified here will be parsed from a Benthos config and will be accessible from the template mapping.").Array().WithChildren(FieldConfigSpec()...),
		docs.FieldBloblang(
			"mapping", "A [Bloblang](/docs/guides/bloblang/about) mapping that translates the fields of the template into a valid Benthos configuration for the target component type.",
		),
		templateMetricsMappingDocs(),
		docs.FieldObject(
			"tests", "Optional unit test definitions for the template that verify certain configurations produce valid configs. These tests are executed with the command `benthos template lint`.",
		).Array().WithChildren(
			docs.FieldString("name", "A name to identify the test."),
			docs.FieldObject("config", "A configuration to run this test with, the config resulting from applying the template with this config will be linted."),
			docs.FieldObject("expected", "An optional configuration describing the expected result of applying the template, when specified the result will be diffed and any mismatching fields will be reported as a test error.").Optional(),
		).HasDefault([]any{}),
	}
}
