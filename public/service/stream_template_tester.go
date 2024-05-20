package service

import (
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/template"
)

// StreamTemplateTester provides utilities for testing templates.
type StreamTemplateTester struct {
	env *Environment
}

// NewStreamTemplateTester creates a component for marshalling stream configs,
// allowing you to print sanitised, redacted or hydrated configs in various
// formats.
func (e *Environment) NewStreamTemplateTester() *StreamTemplateTester {
	return &StreamTemplateTester{
		env: e,
	}
}

// LintYAML attempts to read a template defined in YAML format and lints it.
func (s *StreamTemplateTester) LintYAML(yamlBytes []byte) (lints []Lint, err error) {
	var node yaml.Node
	if err = yaml.Unmarshal(yamlBytes, &node); err != nil {
		return
	}

	for _, l := range template.ConfigSpec().LintYAML(docs.NewLintContext(docs.NewLintConfig(s.env.internal)), &node) {
		lints = append(lints, Lint{
			Line:   l.Line,
			Column: l.Column,
			Type:   convertDocsLintType(l.Type),
			What:   l.What,
		})
	}
	return
}

// RunYAML attempts to read a template defined in YAML format and runs any tests
// defined within the template.
func (s *StreamTemplateTester) RunYAML(yamlBytes []byte) (lints []Lint, err error) {
	var conf template.Config
	if err = yaml.Unmarshal(yamlBytes, &conf); err != nil {
		return
	}

	testErrors, err := conf.Test()
	if err != nil {
		lints = append(lints, Lint{Line: 1, Type: LintFailedRead, What: err.Error()})
		return
	}

	for _, tErr := range testErrors {
		lints = append(lints, Lint{Line: 1, Type: LintFailedRead, What: tErr})
	}
	return
}
