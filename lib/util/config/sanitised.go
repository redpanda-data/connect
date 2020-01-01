package config

import (
	"bytes"
	"encoding/json"
	"sort"
)

//------------------------------------------------------------------------------

// Sanitised is a general map[string]interface{} type that tries to marshal into
// both YAML and JSON in a way that ensure the 'type' field is always first.
type Sanitised map[string]interface{}

// SanitForYAML a map to be embedded within a parent object for YAML
// marshalling.
type SanitForYAML map[string]interface{}

// MarshalYAML return the config as YAML with the 'type' field first.
func (s Sanitised) MarshalYAML() (interface{}, error) {
	dynObj := SanitForYAML{}

	var typeVal interface{}
	for k, v := range s {
		if k == "type" {
			typeVal = v
		} else {
			dynObj[k] = v
		}
	}

	return struct {
		Type         interface{} `yaml:"type"`
		SanitForYAML `yaml:",inline"`
	}{
		Type:         typeVal,
		SanitForYAML: dynObj,
	}, nil
}

// MarshalJSON return the config as a JSON blob with the 'type' field first.
func (s Sanitised) MarshalJSON() ([]byte, error) {
	var keys []string
	var typeVal interface{}

	for k, v := range s {
		if k == "type" {
			typeVal = v
		} else {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')

	if typeVal != nil {
		typeBytes, err := json.Marshal(typeVal)
		if err != nil {
			return nil, err
		}

		buf.WriteString(`"type":`)
		buf.Write(typeBytes)

		if len(keys) > 0 {
			buf.WriteByte(',')
		}
	}

	for i, k := range keys {
		valBytes, err := json.Marshal(s[k])
		if err != nil {
			return nil, err
		}

		buf.WriteByte('"')
		buf.WriteString(k)
		buf.WriteString(`":`)
		buf.Write(valBytes)

		if i < len(keys)-1 {
			buf.WriteByte(',')
		}
	}

	buf.WriteByte('}')
	return buf.Bytes(), nil
}

//------------------------------------------------------------------------------
