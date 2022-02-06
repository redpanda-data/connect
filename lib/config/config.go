package config

import (
	"os"
	"unicode/utf8"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"gopkg.in/yaml.v3"
)

// Type is the Benthos service configuration struct.
type Type struct {
	HTTP                   api.Config `json:"http" yaml:"http"`
	stream.Config          `json:",inline" yaml:",inline"`
	manager.ResourceConfig `json:",inline" yaml:",inline"`
	Logger                 log.Config     `json:"logger" yaml:"logger"`
	Metrics                metrics.Config `json:"metrics" yaml:"metrics"`
	Tracer                 tracer.Config  `json:"tracer" yaml:"tracer"`
	SystemCloseTimeout     string         `json:"shutdown_timeout" yaml:"shutdown_timeout"`
	Tests                  []interface{}  `json:"tests,omitempty" yaml:"tests,omitempty"`
}

// New returns a new configuration with default values.
func New() Type {
	return Type{
		HTTP:               api.NewConfig(),
		Config:             stream.NewConfig(),
		ResourceConfig:     manager.NewResourceConfig(),
		Logger:             log.NewConfig(),
		Metrics:            metrics.NewConfig(),
		Tracer:             tracer.NewConfig(),
		SystemCloseTimeout: "20s",
		Tests:              nil,
	}
}

//------------------------------------------------------------------------------

// ReadV2 will attempt to read a configuration file path into a structure.
// Returns an array of lint messages or an error.
//
// TODO: V4 Remove this and force everything through internal/config
func ReadV2(path string, replaceEnvs, rejectDeprecated bool, config *Type) ([]string, error) {
	configBytes, lints, err := ReadBytes(path, replaceEnvs)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(configBytes, config); err != nil {
		return nil, err
	}

	lintCtx := docs.NewLintContext()
	lintCtx.RejectDeprecated = rejectDeprecated
	newLints, err := LintV2(lintCtx, configBytes)
	if err != nil {
		return nil, err
	}
	lints = append(lints, newLints...)
	return lints, nil
}

// ReadBytes takes a config file path, reads the contents,
// performs a generic parse, resolves any JSON Pointers, marshals the result
// back into bytes and returns it so that it can be unmarshalled into a typed
// structure.
//
// If any non-fatal errors occur lints are returned along with the result.
func ReadBytes(path string, replaceEnvs bool) (configBytes []byte, lints []string, err error) {
	configBytes, err = os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	if !utf8.Valid(configBytes) {
		lints = append(lints, "Detected invalid utf-8 encoding in config, this may result in interpolation functions not working as expected")
	}

	if replaceEnvs {
		configBytes = text.ReplaceEnvVariables(configBytes)
	}

	return configBytes, lints, nil
}
