package service

import (
	"bytes"
	"context"
	"errors"
	"os"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// StreamConfigLinter provides utilities for linting stream configs.
type StreamConfigLinter struct {
	env            *Environment
	spec           docs.FieldSpecs
	lintConf       docs.LintConfig
	skipEnvVarLint bool
	envVarLookupFn func(string) (string, bool)
}

// NewStreamConfigLinter creates a component for marshalling stream configs,
// allowing you to print sanitised, redacted or hydrated configs in various
// formats.
func (s *ConfigSchema) NewStreamConfigLinter() *StreamConfigLinter {
	lintConf := docs.NewLintConfig(s.env.internal)
	return &StreamConfigLinter{
		env:            s.env,
		spec:           s.fields,
		lintConf:       lintConf,
		envVarLookupFn: os.LookupEnv,
	}
}

// SetRejectDeprecated sets whether deprecated fields should trigger linting
// errors.
func (s *StreamConfigLinter) SetRejectDeprecated(v bool) *StreamConfigLinter {
	s.lintConf.RejectDeprecated = v
	return s
}

// SetRequireLabels sets whether labels must be present for all components that
// support them.
func (s *StreamConfigLinter) SetRequireLabels(v bool) *StreamConfigLinter {
	s.lintConf.RequireLabels = v
	return s
}

// SetSkipEnvVarCheck sets whether the linter should ignore cases where
// environment variables are referenced and do not exist.
func (s *StreamConfigLinter) SetSkipEnvVarCheck(v bool) *StreamConfigLinter {
	s.skipEnvVarLint = v
	return s
}

// SetEnvVarLookupFunc overrides the default environment variable lookup so that
// interpolations within a config are resolved by the provided closure function.
func (s *StreamConfigLinter) SetEnvVarLookupFunc(fn func(context.Context, string) (string, bool)) *StreamConfigLinter {
	s.envVarLookupFn = func(s string) (string, bool) {
		return fn(context.Background(), s)
	}
	return s
}

// LintYAML attempts to parse a config in YAML format and, if successful,
// returns a slice of linting errors, or an error is the parsing failed.
func (s *StreamConfigLinter) LintYAML(yamlBytes []byte) (lints []Lint, err error) {
	if !utf8.Valid(yamlBytes) {
		lints = append(lints, Lint{
			Line: 1,
			Type: LintFailedRead,
			What: "detected invalid utf-8 encoding in config, this may result in interpolation functions not working as expected",
		})
	}

	if yamlBytes, err = config.ReplaceEnvVariables(yamlBytes, s.envVarLookupFn); err != nil {
		var errEnvMissing *config.ErrMissingEnvVars
		if !errors.As(err, &errEnvMissing) {
			return
		}
		yamlBytes = errEnvMissing.BestAttempt
		if !s.skipEnvVarLint {
			lints = append(lints, Lint{Line: 1, Type: LintMissingEnvVar, What: err.Error()})
		}
	}

	if bytes.HasPrefix(yamlBytes, []byte("# BENTHOS LINT DISABLE")) {
		return
	}

	var cNode *yaml.Node
	if cNode, err = docs.UnmarshalYAML(yamlBytes); err != nil {
		return
	}

	for _, l := range s.spec.LintYAML(docs.NewLintContext(s.lintConf), cNode) {
		lints = append(lints, Lint{
			Column: l.Column,
			Line:   l.Line,
			Type:   convertDocsLintType(l.Type),
			What:   l.What,
		})
	}
	return
}
