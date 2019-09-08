// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package config

import (
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// Type is the Benthos service configuration struct.
type Type struct {
	HTTP               api.Config `json:"http" yaml:"http"`
	stream.Config      `json:",inline" yaml:",inline"`
	Manager            manager.Config `json:"resources" yaml:"resources"`
	Logger             log.Config     `json:"logger" yaml:"logger"`
	Metrics            metrics.Config `json:"metrics" yaml:"metrics"`
	Tracer             tracer.Config  `json:"tracer" yaml:"tracer"`
	SystemCloseTimeout string         `json:"shutdown_timeout" yaml:"shutdown_timeout"`
}

// New returns a new configuration with default values.
func New() Type {
	return Type{
		HTTP:               api.NewConfig(),
		Config:             stream.NewConfig(),
		Manager:            manager.NewConfig(),
		Logger:             log.NewConfig(),
		Metrics:            metrics.NewConfig(),
		Tracer:             tracer.NewConfig(),
		SystemCloseTimeout: "20s",
	}
}

// SanitisedConfig is a config struct of generic types, this is returned by
// Sanitised() and is a generic structure containing only fields of relevance.
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
}

// Sanitised returns a sanitised copy of the Benthos configuration, meaning
// fields of no consequence (unused inputs, outputs, processors etc) are
// excluded.
func (c Type) Sanitised() (*SanitisedConfig, error) {
	inConf, err := input.SanitiseConfig(c.Input)
	if err != nil {
		return nil, err
	}

	var pipeConf interface{}
	pipeConf, err = pipeline.SanitiseConfig(c.Pipeline)
	if err != nil {
		return nil, err
	}

	var outConf interface{}
	outConf, err = output.SanitiseConfig(c.Output)
	if err != nil {
		return nil, err
	}

	var bufConf interface{}
	bufConf, err = buffer.SanitiseConfig(c.Buffer)
	if err != nil {
		return nil, err
	}

	var mgrConf interface{}
	mgrConf, err = manager.SanitiseConfig(c.Manager)
	if err != nil {
		return nil, err
	}

	var metConf interface{}
	metConf, err = metrics.SanitiseConfig(c.Metrics)
	if err != nil {
		return nil, err
	}

	var tracConf interface{}
	tracConf, err = tracer.SanitiseConfig(c.Tracer)
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
		Logger:             c.Logger,
		Metrics:            metConf,
		Tracer:             tracConf,
		SystemCloseTimeout: c.SystemCloseTimeout,
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
	var inputType, bufferType, conditionType, outputType string
	var processorTypes []string
	for _, e := range examples {
		if _, exists := input.Constructors[e]; exists && len(inputType) == 0 {
			inputType = e
		}
		if _, exists := buffer.Constructors[e]; exists {
			bufferType = e
		}
		if _, exists := processor.Constructors[e]; exists {
			processorTypes = append(processorTypes, e)
		}
		if _, exists := condition.Constructors[e]; exists {
			conditionType = e
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
	if len(conditionType) > 0 {
		condConf := condition.NewConfig()
		condConf.Type = conditionType
		procConf := processor.NewConfig()
		procConf.Type = "filter_parts"
		procConf.FilterParts.Type = conditionType
		conf.Pipeline.Processors = append(conf.Pipeline.Processors, procConf)
	}
	if len(outputType) > 0 {
		conf.Output.Type = outputType
	}
}

//------------------------------------------------------------------------------

// Read will attempt to read a configuration file path into a structure. Returns
// an array of lint messages or an error.
func Read(path string, replaceEnvs bool, config *Type) ([]string, error) {
	configBytes, err := ReadWithJSONPointers(path, replaceEnvs)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(configBytes, config); err != nil {
		return nil, err
	}

	return Lint(configBytes, *config)
}

//------------------------------------------------------------------------------
