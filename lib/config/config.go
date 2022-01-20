package config

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

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

// SanitisedConfig is deprecated and will be removed in V4.
//
// TODO: V4 Remove this
type SanitisedConfig struct {
	HTTP               interface{} `json:"http" yaml:"http"`
	Input              interface{} `json:"input" yaml:"input"`
	Buffer             interface{} `json:"buffer" yaml:"buffer"`
	Pipeline           interface{} `json:"pipeline" yaml:"pipeline"`
	Output             interface{} `json:"output" yaml:"output"`
	Manager            interface{} `json:"resources" yaml:"resources"`
	Logger             interface{} `json:"logger" yaml:"logger"`
	Metrics            interface{} `json:"metrics" yaml:"metrics"`
	Tracer             interface{} `json:"tracer" yaml:"tracer"`
	SystemCloseTimeout interface{} `json:"shutdown_timeout" yaml:"shutdown_timeout"`
	Tests              interface{} `json:"tests,omitempty" yaml:"tests,omitempty"`
}

// SanitisedV2Config describes custom options for how a config is sanitised
// using the V2 API.
type SanitisedV2Config struct {
	RemoveTypeField        bool
	RemoveDeprecatedFields bool
}

// SanitisedV2 returns a sanitised version of the config as a yaml.Node.
func (c Type) SanitisedV2(conf SanitisedV2Config) (yaml.Node, error) {
	var node yaml.Node
	if err := node.Encode(c); err != nil {
		return node, err
	}

	if err := Spec().SanitiseYAML(&node, docs.SanitiseConfig{
		RemoveTypeField:  conf.RemoveTypeField,
		RemoveDeprecated: conf.RemoveDeprecatedFields,
	}); err != nil {
		return node, err
	}

	return node, nil
}

// Sanitised is deprecated and will be removed in V4.
//
// TODO: V4 Remove this
func (c Type) Sanitised() (*SanitisedConfig, error) {
	return c.sanitised(false)
}

// SanitisedNoDeprecated is deprecated and will be removed in V4.
//
// TODO: V4 Remove this
func (c Type) SanitisedNoDeprecated() (*SanitisedConfig, error) {
	return c.sanitised(true)
}

func (c Type) sanitised(skipDeprecated bool) (*SanitisedConfig, error) {
	inConf, err := c.Input.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	var pipeConf interface{}
	pipeConf, err = c.Pipeline.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	var outConf interface{}
	outConf, err = c.Output.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	var bufConf interface{}
	bufConf, err = c.Buffer.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	var mgrConf interface{}
	mgrConf, err = c.Manager.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	var metConf interface{}
	metConf, err = c.Metrics.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	var tracConf interface{}
	tracConf, err = c.Tracer.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	var logConf interface{}
	logConf, err = c.Logger.Sanitised(skipDeprecated)
	if err != nil {
		return nil, err
	}

	return &SanitisedConfig{
		HTTP:               c.HTTP,
		Input:              inConf,
		Buffer:             bufConf,
		Pipeline:           pipeConf,
		Output:             outConf,
		Manager:            mgrConf,
		Logger:             logConf,
		Metrics:            metConf,
		Tracer:             tracConf,
		SystemCloseTimeout: c.SystemCloseTimeout,
		Tests:              c.Tests,
	}, nil
}

//------------------------------------------------------------------------------

// AddExamples takes a configuration struct and a variant list of type names to
// add to it and injects those types appropriately.
//
// For example, your variant arguments could be "kafka" and "amqp", which case
// this function will create a configuration that reads from Kafka and writes
// over AMQP.
func AddExamples(conf *Type, examples ...string) {
	var inputType, bufferType, outputType string
	var processorTypes []string
	for _, e := range examples {
		if _, exists := input.Constructors[e]; exists && inputType == "" {
			inputType = e
		}
		if _, exists := buffer.Constructors[e]; exists {
			bufferType = e
		}
		if _, exists := processor.Constructors[e]; exists {
			processorTypes = append(processorTypes, e)
		}
		if _, exists := output.Constructors[e]; exists {
			outputType = e
		}
	}
	if len(inputType) > 0 {
		conf.Input.Type = inputType
	}
	if len(bufferType) > 0 {
		conf.Buffer.Type = bufferType
	}
	if len(processorTypes) > 0 {
		for _, procType := range processorTypes {
			procConf := processor.NewConfig()
			procConf.Type = procType
			conf.Pipeline.Processors = append(conf.Pipeline.Processors, procConf)
		}
	}
	if len(outputType) > 0 {
		conf.Output.Type = outputType
	}
}

//------------------------------------------------------------------------------

// Read will attempt to read a configuration file path into a structure. Returns
// an array of lint messages or an error.
//
// TODO: V4 Remove this and force everything through internal/config
func Read(path string, replaceEnvs bool, config *Type) ([]string, error) {
	return ReadV2(path, replaceEnvs, false, config)
}

// ReadV2 will attempt to read a configuration file path into a structure.
// Returns an array of lint messages or an error.
//
// TODO: V4 Remove this and force everything through internal/config
func ReadV2(path string, replaceEnvs, rejectDeprecated bool, config *Type) ([]string, error) {
	configBytes, lints, err := ReadWithJSONPointersLinted(path, replaceEnvs)
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
