package output

import (
	"encoding/json"

	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

type brokerOutputList []Config

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerOutputList) UnmarshalJSON(bytes []byte) error {
	genericOutputs := []interface{}{}
	if err := json.Unmarshal(bytes, &genericOutputs); err != nil {
		return err
	}

	outputConfs, err := parseOutputConfsWithDefaults(genericOutputs)
	if err != nil {
		return err
	}

	*b = outputConfs
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerOutputList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	genericOutputs := []interface{}{}
	if err := unmarshal(&genericOutputs); err != nil {
		return err
	}

	outputConfs, err := parseOutputConfsWithDefaults(genericOutputs)
	if err != nil {
		return err
	}

	*b = outputConfs
	return nil
}

//------------------------------------------------------------------------------

// parseOutputConfsWithDefaults takes an array of output configs as
// []interface{} and returns an array of output configs with default values in
// place of omitted values. This is necessary because when unmarshalling config
// files using structs you can pre-populate non-reference type struct fields
// with default values, but array objects will lose those defaults.
//
// In order to ensure that omitted values are set to default we initially parse
// the array as interface{} types and then individually apply the defaults by
// marshalling and unmarshalling. The correct way to do this would be to use
// json.RawMessage, but our config files can contain a range of different
// formats that we do not know at this stage (JSON, YAML, etc), therefore we use
// the more hacky method as performance is not an issue at this stage.
//
// TODO: V4 Remove this?
func parseOutputConfsWithDefaults(outConfs []interface{}) ([]Config, error) {
	outputConfs := []Config{}

	for _, boxedConfig := range outConfs {
		newConfs := make([]Config, 1)
		for _, conf := range newConfs {
			rawBytes, err := yaml.Marshal(boxedConfig)
			if err != nil {
				return nil, err
			}
			if err := yaml.Unmarshal(rawBytes, &conf); err != nil {
				return nil, err
			}
			outputConfs = append(outputConfs, conf)
		}
	}

	return outputConfs, nil
}

//------------------------------------------------------------------------------
