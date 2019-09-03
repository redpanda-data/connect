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

package ratelimit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each ratelimit type.
type TypeSpec struct {
	constructor func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.RateLimit, error)
	description string
}

// Constructors is a map of all cache types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each ratelimit type.
const (
	TypeLocal = "local"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all cache types.
type Config struct {
	Type   string      `json:"type" yaml:"type"`
	Local  LocalConfig `json:"local" yaml:"local"`
	Plugin interface{} `json:"plugin,omitempty" yaml:"plugin,omitempty"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:   "local",
		Local:  NewLocalConfig(),
		Plugin: nil,
	}
}

//------------------------------------------------------------------------------

// SanitiseConfig creates a sanitised version of a config.
func SanitiseConfig(conf Config) (interface{}, error) {
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	outputMap := config.Sanitised{}
	outputMap["type"] = conf.Type

	if _, exists := hashMap[conf.Type]; exists {
		outputMap[conf.Type] = hashMap[conf.Type]
	}
	if spec, exists := pluginSpecs[conf.Type]; exists {
		var plugSanit interface{}
		if spec.confSanitiser != nil {
			plugSanit = spec.confSanitiser(conf.Plugin)
		} else {
			plugSanit = hashMap["plugin"]
		}
		if plugSanit != nil {
			outputMap["plugin"] = plugSanit
		}
	}

	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := value.Decode(&aliased); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var raw interface{}
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}
	if typeCandidates := config.GetInferenceCandidates(raw); len(typeCandidates) > 0 {
		var inferredType string
		for _, tc := range typeCandidates {
			if _, exists := Constructors[tc]; exists {
				if len(inferredType) > 0 {
					return fmt.Errorf("line %v: unable to infer type, multiple candidates '%v' and '%v'", value.Line, inferredType, tc)
				}
				inferredType = tc
			}
		}
		if len(inferredType) == 0 {
			return fmt.Errorf("line %v: unable to infer type, candidates were: %v", value.Line, typeCandidates)
		}
		aliased.Type = inferredType
	}

	if spec, exists := pluginSpecs[aliased.Type]; exists && spec.confConstructor != nil {
		confBytes, err := yaml.Marshal(aliased.Plugin)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}

		conf := spec.confConstructor()
		if err = yaml.Unmarshal(confBytes, conf); err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		aliased.Plugin = conf
	} else {
		aliased.Plugin = nil
	}

	*c = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-rate-limits`" + `

A rate limit is a strategy for limiting the usage of a shared resource across
parallel components in a Benthos instance, or potentially across multiple
instances.

For example, if we wanted to protect an HTTP service with a local rate limit
we could configure one like so:

` + "``` yaml" + `
input:
  type: foo
pipeline:
  threads: 8
  processors:
  - http:
      request:
        url: http://foo.bar/baz
        rate_limit: foobar
      parallel: true
resources:
  rate_limits:
    foobar:
      type: local
      local:
        count: 500
        interval: 1s
` + "```" + `

In this example if the messages from the input ` + "`foo`" + ` are batches the
requests of a batch will be sent in parallel. This is usually going to be what
we want, but could potentially stress our HTTP server if a batch is large.

However, by using a rate limit we can guarantee that even across parallel
processing pipelines and variable sized batches we wont hit the service more
than 500 times per second.`

// Descriptions returns a formatted string of descriptions for each type.
func Descriptions() string {
	// Order our rate limit types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Rate Limits\n")
	buf.WriteString(strings.Repeat("=", 11))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	buf.WriteString("### Contents\n\n")
	for i, name := range names {
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i+1, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = config.MarshalYAML(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
		buf.WriteString(Constructors[name].description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

// New creates a rate limit type based on an rate limit configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (types.RateLimit, error) {
	if c, ok := Constructors[conf.Type]; ok {
		rl, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create rate limit '%v': %v", conf.Type, err)
		}
		return rl, nil
	}
	if c, ok := pluginSpecs[conf.Type]; ok {
		rl, err := c.constructor(conf.Plugin, mgr, log.NewModule("."+conf.Type), stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create rate limit '%v': %v", conf.Type, err)
		}
		return rl, nil
	}
	return nil, types.ErrInvalidRateLimitType
}

//------------------------------------------------------------------------------
