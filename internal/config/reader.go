package config

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/gabs/v2"
	"gopkg.in/yaml.v3"
)

// Reader provides utilities for parsing a Benthos config as a main file with
// a collection of resource files, and options such as overrides.
type Reader struct {
	mainPath      string
	resourcePaths []string
	overrides     []string
}

// NewReader creates a new config reader.
func NewReader(mainPath string, resourcePaths []string, opts ...OptFunc) *Reader {
	r := &Reader{
		mainPath:      mainPath,
		resourcePaths: resourcePaths,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

//------------------------------------------------------------------------------

// OptFunc is an opt function that changes the behaviour of a config reader.
type OptFunc func(*Reader)

// OptAddOverrides adds one or more override expressions to the config reader,
// each of the form `path=value`.
func OptAddOverrides(overrides ...string) OptFunc {
	return func(r *Reader) {
		r.overrides = append(r.overrides, overrides...)
	}
}

//------------------------------------------------------------------------------

func applyOverrides(specs docs.FieldSpecs, root *yaml.Node, overrides ...string) error {
	for _, override := range overrides {
		eqIndex := strings.Index(override, "=")
		if eqIndex == -1 {
			return fmt.Errorf("invalid set expression '%v': expected foo=bar syntax", override)
		}

		path := override[:eqIndex]
		value := override[eqIndex+1:]
		if path == "" || value == "" {
			return fmt.Errorf("invalid set expression '%v': expected foo=bar syntax", override)
		}

		valNode := yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: value,
		}
		if err := specs.SetYAMLPath(nil, root, &valNode, gabs.DotPathToSlice(path)...); err != nil {
			return fmt.Errorf("failed to set config field override: %w", err)
		}
	}
	return nil
}

func (r *Reader) readMain(conf *config.Type) (lints []string, err error) {
	defer func() {
		if err != nil && r.mainPath != "" {
			err = fmt.Errorf("%v: %w", r.mainPath, err)
		}
	}()

	if r.mainPath == "" && len(r.overrides) == 0 {
		return
	}

	var rawNode yaml.Node
	var confBytes []byte
	if r.mainPath != "" {
		if confBytes, lints, err = config.ReadWithJSONPointersLinted(r.mainPath, true); err != nil {
			return
		}
		if err = yaml.Unmarshal(confBytes, &rawNode); err != nil {
			return
		}
	}

	confSpec := config.Spec()
	if err = applyOverrides(confSpec, &rawNode, r.overrides...); err != nil {
		return
	}

	if !bytes.HasPrefix(confBytes, []byte("# BENTHOS LINT DISABLE")) {
		lintFilePrefix := ""
		if r.mainPath != "" {
			lintFilePrefix = fmt.Sprintf("%v: ", r.mainPath)
		}
		for _, lint := range confSpec.LintYAML(docs.NewLintContext(), &rawNode) {
			lints = append(lints, fmt.Sprintf("%vline %v: %v", lintFilePrefix, lint.Line, lint.What))
		}
	}

	err = rawNode.Decode(conf)
	return
}

func (r *Reader) readResources(conf *config.Type) (lints []string, err error) {
	for _, path := range r.resourcePaths {
		rconf := manager.NewResourceConfig()
		var rLints []string
		if rLints, err = readResource(path, &rconf); err != nil {
			return
		}
		lints = append(lints, rLints...)
		if err = conf.ResourceConfig.AddFrom(&rconf); err != nil {
			err = fmt.Errorf("%v: %w", path, err)
			return
		}
	}
	return
}

func readResource(path string, conf *manager.ResourceConfig) (lints []string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%v: %w", path, err)
		}
	}()

	var confBytes []byte
	if confBytes, lints, err = config.ReadWithJSONPointersLinted(path, true); err != nil {
		return
	}

	var rawNode yaml.Node
	if err = yaml.Unmarshal(confBytes, &rawNode); err != nil {
		return
	}
	if !bytes.HasPrefix(confBytes, []byte("# BENTHOS LINT DISABLE")) {
		for _, lint := range manager.Spec().LintYAML(docs.NewLintContext(), &rawNode) {
			lints = append(lints, fmt.Sprintf("resource file %v: line %v: %v", path, lint.Line, lint.What))
		}
	}

	err = rawNode.Decode(conf)
	return
}

// Read a Benthos config from the files and options specified.
func (r *Reader) Read(conf *config.Type) (lints []string, err error) {
	if lints, err = r.readMain(conf); err != nil {
		return
	}
	var rLints []string
	if rLints, err = r.readResources(conf); err != nil {
		return
	}
	lints = append(lints, rLints...)
	return
}
