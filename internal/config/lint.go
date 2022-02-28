package config

import (
	"bytes"
	"fmt"
	"os"
	"unicode/utf8"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"gopkg.in/yaml.v3"
)

// ReadFileLinted will attempt to read a configuration file path into a
// structure. Returns an array of lint messages or an error.
func ReadFileLinted(path string, rejectDeprecated bool, config *Type) ([]string, error) {
	configBytes, lints, err := ReadFileEnvSwap(path)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(configBytes, config); err != nil {
		return nil, err
	}

	lintCtx := docs.NewLintContext()
	lintCtx.RejectDeprecated = rejectDeprecated
	newLints, err := LintBytes(lintCtx, configBytes)
	if err != nil {
		return nil, err
	}
	lints = append(lints, newLints...)
	return lints, nil
}

// LintBytes attempts to report errors within a user config. Returns a slice of
// lint results.
func LintBytes(ctx docs.LintContext, rawBytes []byte) ([]string, error) {
	if bytes.HasPrefix(rawBytes, []byte("# BENTHOS LINT DISABLE")) {
		return nil, nil
	}

	var rawNode yaml.Node
	if err := yaml.Unmarshal(rawBytes, &rawNode); err != nil {
		return nil, err
	}

	var lintStrs []string
	for _, lint := range Spec().LintYAML(ctx, &rawNode) {
		if lint.Level == docs.LintError {
			lintStrs = append(lintStrs, fmt.Sprintf("line %v: %v", lint.Line, lint.What))
		}
	}
	return lintStrs, nil
}

// ReadFileEnvSwap reads a file and replaces any environment variable
// interpolations before returning the contents. Linting errors are returned if
// the file has an unexpected higher level format, such as invalid utf-8
// encoding.
func ReadFileEnvSwap(path string) (configBytes []byte, lints []string, err error) {
	configBytes, err = os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	if !utf8.Valid(configBytes) {
		lints = append(lints, "Detected invalid utf-8 encoding in config, this may result in interpolation functions not working as expected")
	}

	configBytes = ReplaceEnvVariables(configBytes)
	return configBytes, lints, nil
}
