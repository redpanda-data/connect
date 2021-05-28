package template

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/fatih/color"
	"github.com/nsf/jsondiff"
	"gopkg.in/yaml.v3"
)

// FieldConfig describes a configuration field used in the template.
type FieldConfig struct {
	Name        string       `yaml:"name"`
	Description string       `yaml:"description"`
	Type        *string      `yaml:"type,omitempty"`
	Kind        *string      `yaml:"kind,omitempty"`
	Default     *interface{} `yaml:"default,omitempty"`
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
	Summary        string        `yaml:"summary"`
	Description    string        `yaml:"description"`
	Fields         []FieldConfig `yaml:"fields"`
	Mapping        string        `yaml:"mapping"`
	MetricsMapping string        `yaml:"metrics_mapping"`
	Tests          []TestConfig  `yaml:"tests"`
}

// FieldSpec creates a documentation field spec from a template field config.
func (c FieldConfig) FieldSpec() (docs.FieldSpec, error) {
	f := docs.FieldCommon(c.Name, c.Description)
	if c.Default != nil {
		f = f.HasDefault(*f.Default)
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

	return docs.ComponentSpec{
		Name:        c.Name,
		Type:        docs.Type(c.Type),
		Status:      docs.StatusPlugin,
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
	mapping, err := bloblang.NewMapping("", c.Mapping)
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
	return &compiled{spec, mapping, metricsMapping}, nil
}

func diffYAMLNodesAsJSON(expNode, actNode *yaml.Node) (string, error) {
	var iexp, iact interface{}
	if err := expNode.Decode(&iexp); err != nil {
		return "", fmt.Errorf("failed to marshal expected %w", err)
	}
	if err := actNode.Decode(&iact); err != nil {
		return "", fmt.Errorf("failed to marshal actual %w", err)
	}

	expBytes, err := json.Marshal(iexp)
	if err != nil {
		return "", fmt.Errorf("failed to marshal expected %w", err)
	}
	actBytes, err := json.Marshal(iact)
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
		outConf, err := compiled.ExpandToNode(&test.Config)
		if err != nil {
			return nil, fmt.Errorf("test '%v': %w", test.Name, err)
		}
		for _, lint := range docs.LintNode(docs.NewLintContext(), docs.Type(c.Type), outConf) {
			failures = append(failures, fmt.Sprintf("test '%v': lint error in resulting config: %v", test.Name, lint.What))
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

// ReadConfig attempts to read a template configuration file.
func ReadConfig(path string) (conf Config, lints []string, err error) {
	var templateBytes []byte
	if templateBytes, err = ioutil.ReadFile(path); err != nil {
		return
	}

	if err = yaml.Unmarshal(templateBytes, &conf); err != nil {
		return
	}

	var node yaml.Node
	if err = yaml.Unmarshal(templateBytes, &node); err != nil {
		return
	}

	for _, l := range ConfigSpec().LintNode(docs.NewLintContext(), node.Content[0]) {
		if l.Level == docs.LintError {
			lints = append(lints, fmt.Sprintf("line %v: %v", l.Line, l.What))
		}
	}

	return
}

//------------------------------------------------------------------------------

// FieldConfigSpec returns a configuration spec for a field of a template.
func FieldConfigSpec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("name", "The name of the field.").HasType(docs.FieldString),
		docs.FieldCommon("description", "A description of the field.").HasType(docs.FieldString),
		docs.FieldCommon("type", "The scalar type of the field.").HasType(docs.FieldString).HasOptions(
			"string", "int", "float", "bool",
		).LintOptions(),
		docs.FieldCommon("kind", "The kind of the field.").HasType(docs.FieldString).HasOptions(
			"scalar", "map", "list",
		).HasDefault("scalar").LintOptions(),
		docs.FieldCommon("default", "An optional default value for the field. If a default value is not specified then a configuration without the field is considered incorrect."),
	}
}

// ConfigSpec returns a configuration spec for a template.
func ConfigSpec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("name", "The name of the component this template will create.").HasType(docs.FieldString),
		docs.FieldCommon("type", "The type of the component this template will create.").HasOptions(
			"cache", "input", "output", "processor", "rate_limit",
		).HasType(docs.FieldString),
		docs.FieldCommon("summary", "A short summary of the component.").HasType(docs.FieldString),
		docs.FieldCommon("description", "A longer form description of the component and how to use it.").HasType(docs.FieldString),
		docs.FieldCommon("fields", "The configuration fields of the template, fields specified here will be parsed from a Benthos config and will be accessible from the template mapping.").Array().WithChildren(FieldConfigSpec()...),
		docs.FieldCommon(
			"mapping", "A [Bloblang](/docs/guides/bloblang/about) mapping that translates the fields of the template into a valid Benthos configuration for the target component type.",
		).Linter(docs.LintBloblangMapping).HasType(docs.FieldString),
		metrics.MappingFieldSpec(),
		docs.FieldCommon(
			"tests", "Optional unit test definitions for the template that verify certain configurations produce valid configs. These tests are executed with the command `benthos template lint`.",
		).Array().WithChildren(
			docs.FieldCommon("name", "A name to identify the test.").HasType(docs.FieldString),
			docs.FieldCommon("config", "A configuration to run this test with, the config resulting from applying the template with this config will be linted.").HasType(docs.FieldObject),
			docs.FieldCommon("expected", "An optional configuration describing the expected result of applying the template, when specified the result will be diffed and any mismatching fields will be reported as a test error.").HasType(docs.FieldObject),
		),
	}
}
