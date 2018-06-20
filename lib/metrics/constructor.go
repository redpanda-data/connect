// Copyright (c) 2014 Ashley Jeffs
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

package metrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
)

//------------------------------------------------------------------------------

// Errors for the metrics package.
var (
	ErrInvalidMetricOutputType = errors.New("invalid metrics output type")
)

//------------------------------------------------------------------------------

// typeSpec is a constructor and a usage description for each metric output
// type.
type typeSpec struct {
	constructor func(conf Config, opts ...func(Type)) (Type, error)
	description string
}

var constructors = map[string]typeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all metric output
// types.
type Config struct {
	Type       string       `json:"type" yaml:"type"`
	Prefix     string       `json:"prefix" yaml:"prefix"`
	HTTP       struct{}     `json:"http_server" yaml:"http_server"`
	Prometheus struct{}     `json:"prometheus" yaml:"prometheus"`
	Statsd     StatsdConfig `json:"statsd" yaml:"statsd"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:       "http_server",
		Prefix:     "benthos",
		HTTP:       struct{}{},
		Prometheus: struct{}{},
		Statsd:     NewStatsdConfig(),
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	outputMap := map[string]interface{}{}
	outputMap["type"] = hashMap["type"]
	outputMap[conf.Type] = hashMap[conf.Type]

	return outputMap, nil
}

//------------------------------------------------------------------------------

// OptSetLogger sets the logging output to be used by the metrics clients.
func OptSetLogger(log log.Modular) func(Type) {
	return func(t Type) {
		t.SetLogger(log)
	}
}

//------------------------------------------------------------------------------

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our input types alphabetically
	names := []string{}
	for name := range constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("METRIC TARGETS\n")
	buf.WriteString(strings.Repeat("=", 80))
	buf.WriteString("\n\n")
	buf.WriteString("This document has been generated with `benthos --list-metrics`.")
	buf.WriteString("\n\n")

	// Append each description
	for i, name := range names {
		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		buf.WriteString(constructors[name].description)
		if i != (len(names) - 1) {
			buf.WriteString("\n\n")
		}
	}
	return buf.String()
}

// New creates a metric output type based on a configuration.
func New(conf Config, opts ...func(Type)) (Type, error) {
	if conf.Type == "none" {
		return DudType{}, nil
	}
	if c, ok := constructors[conf.Type]; ok {
		return c.constructor(conf, opts...)
	}
	return nil, ErrInvalidMetricOutputType
}

//------------------------------------------------------------------------------
