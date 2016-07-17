/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package input

import (
	"bytes"
	"sort"
	"strings"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

// typeSpec - Constructor and a usage description for each input type.
type typeSpec struct {
	constructor func(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error)
	description string
}

var constructors = map[string]typeSpec{}

//--------------------------------------------------------------------------------------------------

// Config - The all encompassing configuration struct for all input types. Note that some configs
// are empty structs, as the type has no optional values but we want to list it as an option.
type Config struct {
	Type       string           `json:"type" yaml:"type"`
	HTTPServer HTTPServerConfig `json:"http_server" yaml:"http_server"`
	ScaleProto ScaleProtoConfig `json:"scalability_protocols" yaml:"scalability_protocols"`
	ZMQ4       *ZMQ4Config      `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Kafka      KafkaConfig      `json:"kafka" yaml:"kafka"`
	File       FileConfig       `json:"file" yaml:"file"`
	STDIN      struct{}         `json:"stdin" yaml:"stdin"`
	FanIn      FanInConfig      `json:"fan_in" yaml:"fan_in"`
}

// NewConfig - Returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:       "stdin",
		HTTPServer: NewHTTPServerConfig(),
		ScaleProto: NewScaleProtoConfig(),
		ZMQ4:       NewZMQ4Config(),
		Kafka:      NewKafkaConfig(),
		File:       NewFileConfig(),
		STDIN:      struct{}{},
		FanIn:      NewFanInConfig(),
	}
}

//--------------------------------------------------------------------------------------------------

// Descriptions - Returns a formatted string of collated descriptions of each type.
func Descriptions() string {
	// Order our input types alphabetically
	names := []string{}
	for name := range constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("INPUTS\n")
	buf.WriteString(strings.Repeat("=", 80))
	buf.WriteString("\n\n")

	// Append each description
	for i, name := range names {
		buf.WriteString(name)
		buf.WriteString("\n")
		buf.WriteString(strings.Repeat("-", 80))
		buf.WriteString("\n")
		buf.WriteString(constructors[name].description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

// Construct - Create an input type based on an input configuration.
func Construct(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	if c, ok := constructors[conf.Type]; ok {
		return c.constructor(conf, log, stats)
	}
	return nil, types.ErrInvalidInputType
}

//--------------------------------------------------------------------------------------------------
