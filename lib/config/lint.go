package config

import (
	"bytes"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"gopkg.in/yaml.v3"
)

// Lint attempts to report errors within a user config. Returns a slice of lint
// results.
func Lint(rawBytes []byte, _ Type) ([]string, error) {
	if bytes.HasPrefix(rawBytes, []byte("# BENTHOS LINT DISABLE")) {
		return nil, nil
	}

	var rawNode yaml.Node
	if err := yaml.Unmarshal(rawBytes, &rawNode); err != nil {
		return nil, err
	}

	var lintStrs []string
	for _, lint := range Spec().LintYAML(docs.NewLintContext(), &rawNode) {
		if lint.Level == docs.LintError {
			lintStrs = append(lintStrs, fmt.Sprintf("line %v: %v", lint.Line, lint.What))
		}
	}
	return lintStrs, nil
}
