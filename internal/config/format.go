package config

import (
	"bytes"

	"gopkg.in/yaml.v3"
)

// MarshalYAML marshals a structure into YAML with consistent formatting across
// all Benthos components.
func MarshalYAML(v yaml.Node) ([]byte, error) {
	var cbytes bytes.Buffer
	enc := yaml.NewEncoder(&cbytes)
	enc.SetIndent(2)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return cbytes.Bytes(), nil
}
