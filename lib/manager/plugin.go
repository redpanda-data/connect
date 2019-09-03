// Copyright (c) 2019 Ashley Jeffs
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

package manager

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

// PluginConfig is a config struct representing a resource plugin.
type PluginConfig struct {
	Type   string      `json:"type" yaml:"type"`
	Plugin interface{} `json:"plugin" yaml:"plugin"`
}

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (p *PluginConfig) UnmarshalJSON(bytes []byte) error {
	type confAlias PluginConfig
	aliased := confAlias(PluginConfig{})

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	if spec, exists := pluginSpecs[aliased.Type]; exists {
		dummy := struct {
			Conf interface{} `json:"plugin"`
		}{
			Conf: spec.confConstructor(),
		}
		if err := json.Unmarshal(bytes, &dummy); err != nil {
			return fmt.Errorf("failed to parse plugin config: %v", err)
		}
		aliased.Plugin = dummy.Conf
	} else {
		return fmt.Errorf("plugin type '%s' not recognised", aliased.Type)
	}

	*p = PluginConfig(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (p *PluginConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias PluginConfig
	aliased := confAlias(PluginConfig{})

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	if spec, exists := pluginSpecs[aliased.Type]; exists {
		confBytes, err := yaml.Marshal(aliased.Plugin)
		if err != nil {
			return err
		}

		conf := spec.confConstructor()
		if err = yaml.Unmarshal(confBytes, conf); err != nil {
			return err
		}
		aliased.Plugin = conf
	} else {
		return fmt.Errorf("plugin type '%s' not recognised", aliased.Type)
	}

	*p = PluginConfig(aliased)
	return nil
}

//------------------------------------------------------------------------------

// PluginConstructor is a func that constructs a Benthos resource plugin. These
// are shareable resources that can be accessed by any other type within
// Benthos.
//
// The configuration object will be the result of the PluginConfigConstructor
// after overlaying the user configuration.
type PluginConstructor func(
	config interface{},
	manager types.Manager,
	logger log.Modular,
	metrics metrics.Type,
) (interface{}, error)

// PluginConfigConstructor is a func that returns a pointer to a new and fully
// populated configuration struct for a plugin type. It is valid to return a
// pointer to an empty struct (&struct{}{}) if no configuration fields are
// needed.
type PluginConfigConstructor func() interface{}

// PluginConfigSanitiser is a function that takes a configuration object for a
// plugin and returns a sanitised (minimal) version of it for printing in
// examples and plugin documentation.
//
// This function is useful for when a plugins configuration struct is very large
// and complex, but can sometimes be expressed in a more concise way without
// losing the original intent.
type PluginConfigSanitiser func(conf interface{}) interface{}

type pluginSpec struct {
	constructor     PluginConstructor
	confConstructor PluginConfigConstructor
	confSanitiser   PluginConfigSanitiser
	description     string
}

// pluginSpecs is a map of all resource plugin type specs.
var pluginSpecs = map[string]pluginSpec{}

// RegisterPlugin registers a plugin by a unique name so that it can be
// constucted similar to regular resources. A constructor for both the plugin
// itself as well as its configuration struct must be provided.
//
// A constructed resource plugin can be any type and is wrapped as an
// interface{} type.
func RegisterPlugin(
	typeString string,
	configConstructor PluginConfigConstructor,
	constructor PluginConstructor,
) {
	spec := pluginSpecs[typeString]
	spec.constructor = constructor
	spec.confConstructor = configConstructor
	pluginSpecs[typeString] = spec
}

// DocumentPlugin adds a description and an optional configuration sanitiser
// function to the definition of a registered plugin. This improves the
// documentation generated by PluginDescriptions.
func DocumentPlugin(
	typeString, description string,
	configSanitiser PluginConfigSanitiser,
) {
	spec := pluginSpecs[typeString]
	spec.description = description
	spec.confSanitiser = configSanitiser
	pluginSpecs[typeString] = spec
}

//------------------------------------------------------------------------------

var pluginHeader = `This document has been generated, do not edit it directly.

This document lists any resource plugins that this flavour of Benthos offers.`

// PluginDescriptions generates and returns a markdown formatted document
// listing each registered plugin and an example configuration for it.
func PluginDescriptions() string {
	// Order alphabetically
	names := []string{}
	for name := range pluginSpecs {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Resource Plugins\n")
	buf.WriteString(strings.Repeat("=", 16))
	buf.WriteString("\n\n")
	buf.WriteString(pluginHeader)
	buf.WriteString("\n\n")

	buf.WriteString("### Contents\n\n")
	for i, name := range names {
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i+1, name, name))
	}

	if len(names) == 0 {
		buf.WriteString("There are no plugins loaded.")
	} else {
		buf.WriteString("\n")
	}

	// Append each description
	for i, name := range names {
		var confBytes []byte

		plugConf := PluginConfig{
			Type:   name,
			Plugin: pluginSpecs[name].confConstructor(),
		}
		conf := NewConfig()
		conf.Plugins["example"] = plugConf
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
		if desc := pluginSpecs[name].description; len(desc) > 0 {
			buf.WriteString("\n")
			buf.WriteString(desc)
			buf.WriteString("\n")
		}
		if i != (len(names) - 1) {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

//------------------------------------------------------------------------------
