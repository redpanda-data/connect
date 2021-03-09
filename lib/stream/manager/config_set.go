package manager

import (
	"github.com/Jeffail/benthos/v3/lib/stream"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// ConfigSet is a map of stream configurations mapped by ID, which can be YAML
// parsed without losing default values inside the stream configs.
type ConfigSet map[string]stream.Config

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (c ConfigSet) UnmarshalYAML(unmarshal func(interface{}) error) error {
	tmpSet := map[string]interface{}{}
	if err := unmarshal(&tmpSet); err != nil {
		return err
	}

	for k, v := range tmpSet {
		conf := stream.NewConfig()
		confBytes, err := yaml.Marshal(v)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(confBytes, &conf); err != nil {
			return err
		}
		c[k] = conf
	}

	return nil
}

//------------------------------------------------------------------------------
