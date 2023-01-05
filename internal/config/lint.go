package config

import (
	"bytes"
	"io"
	"io/fs"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

// LintOptions specifies the linters that will be enabled.
type LintOptions struct {
	RejectDeprecated bool
	RequireLabels    bool
}

// ReadFileLinted will attempt to read a configuration file path into a
// structure. Returns an array of lint messages or an error.
func ReadFileLinted(fs ifs.FS, path string, opts LintOptions, config *Type) ([]docs.Lint, error) {
	configBytes, lints, _, err := ReadFileEnvSwap(fs, path)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(configBytes, config); err != nil {
		return nil, err
	}

	newLints, err := LintBytes(opts, configBytes)
	if err != nil {
		return nil, err
	}
	lints = append(lints, newLints...)
	return lints, nil
}

// LintBytes attempts to report errors within a user config. Returns a slice of
// lint results.
func LintBytes(opts LintOptions, rawBytes []byte) ([]docs.Lint, error) {
	if bytes.HasPrefix(rawBytes, []byte("# BENTHOS LINT DISABLE")) {
		return nil, nil
	}

	var rawNode yaml.Node
	if err := yaml.Unmarshal(rawBytes, &rawNode); err != nil {
		return nil, err
	}

	lintCtx := docs.NewLintContext()
	lintCtx.RejectDeprecated = opts.RejectDeprecated
	lintCtx.RequireLabels = opts.RequireLabels

	return Spec().LintYAML(lintCtx, &rawNode), nil
}

// ReadFileEnvSwap reads a file and replaces any environment variable
// interpolations before returning the contents. Linting errors are returned if
// the file has an unexpected higher level format, such as invalid utf-8
// encoding.
//
// An modTime timestamp is returned if the modtime of the file is available.
func ReadFileEnvSwap(store ifs.FS, path string) (configBytes []byte, lints []docs.Lint, modTime time.Time, err error) {
	var configFile fs.File
	if configFile, err = store.Open(path); err != nil {
		return
	}

	if info, ierr := configFile.Stat(); ierr == nil {
		modTime = info.ModTime()
	}

	if configBytes, err = io.ReadAll(configFile); err != nil {
		return
	}

	if !utf8.Valid(configBytes) {
		lints = append(lints, docs.NewLintError(
			1, docs.LintFailedRead,
			"Detected invalid utf-8 encoding in config, this may result in interpolation functions not working as expected",
		))
	}

	configBytes = ReplaceEnvVariables(configBytes)
	return
}
