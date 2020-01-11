package docs

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"

	"github.com/Jeffail/benthos/v3/lib/util/config"
	"github.com/Jeffail/gabs/v2"
)

// ComponentSpec describes a Benthos component.
type ComponentSpec struct {
	// Name of the component
	Name string

	// Type of the component (input, output, etc)
	Type string

	// Description of the component (in markdown).
	Description string

	Fields FieldSpecs
}

// AsMarkdown renders the spec of a component, along with a full configuration
// example, into a markdown document.
func (c *ComponentSpec) AsMarkdown(fullConfigExample interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("---\ntitle: %v\n---\n\n", c.Name))
	buf.WriteString("# `" + c.Name + "`")
	buf.WriteString("\n\n")

	var advancedConfigBytes []byte
	var advancedConfig map[string]interface{}
	var err error

	if asMap, isMap := fullConfigExample.(map[string]interface{}); isMap {
		advancedConfig = c.Fields.ConfigAdvanced(asMap)
		if advancedConfigBytes, err = config.MarshalYAML(map[string]interface{}{
			c.Type: map[string]interface{}{
				c.Name: advancedConfig,
			},
		}); err != nil {
			return nil, err
		}
	} else {
		if advancedConfigBytes, err = config.MarshalYAML(map[string]interface{}{
			c.Type: map[string]interface{}{
				c.Name: fullConfigExample,
			},
		}); err != nil {
			return nil, err
		}
	}
	buf.WriteString("```yaml\n")
	buf.Write(advancedConfigBytes)
	buf.WriteString("```")
	buf.WriteString("\n\n")

	if c.Description[0] == '\n' {
		c.Description = c.Description[1:]
	}
	buf.WriteString(c.Description)
	buf.WriteString("\n\n")
	if advancedConfig == nil {
		return buf.Bytes(), nil
	}

	buf.WriteString("### Fields")
	buf.WriteString("\n\n")

	fieldNames := []string{}
	unrecognisedSpecs := []string{}
	for k, spec := range c.Fields {
		if spec.Deprecated {
			continue
		}
		fieldNames = append(fieldNames, k)
		if _, exists := advancedConfig[k]; !exists {
			unrecognisedSpecs = append(unrecognisedSpecs, k)
		}
	}
	if len(unrecognisedSpecs) > 0 {
		return nil, fmt.Errorf("unrecognised fields found within '%v' spec: %v", c.Name, unrecognisedSpecs)
	}
	for k := range advancedConfig {
		if _, exists := c.Fields[k]; !exists {
			fieldNames = append(fieldNames, k)
		}
	}
	sort.Strings(fieldNames)

	for _, k := range fieldNames {
		v, hasSpec := c.Fields[k]
		if v.Deprecated {
			continue
		}
		buf.WriteString("### `" + k + "`")
		buf.WriteString("\n\n")

		fieldType := reflect.TypeOf(advancedConfig[k]).Kind().String()
		switch fieldType {
		case "map":
			fieldType = "object"
		case "slice":
			fieldType = "array"
		}
		buf.WriteString(fmt.Sprintf("Type: `%v`\n\n", fieldType))
		if !hasSpec {
			continue
		}
		buf.WriteString(v.Description)
		buf.WriteString("\n\n")
		if len(v.Examples) > 0 {
			buf.WriteString("Examples:\n\n")
			for _, e := range v.Examples {
				buf.WriteString("- `" + gabs.Wrap(e).String() + "`")
			}
		}
	}

	return buf.Bytes(), nil
}
