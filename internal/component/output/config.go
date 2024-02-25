package output

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config is the all encompassing configuration struct for all output types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
type Config struct {
	Label      string             `json:"label" yaml:"label"`
	Type       string             `json:"type" yaml:"type"`
	Plugin     any                `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Processors []processor.Config `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
func NewConfig() Config {
	return Config{
		Label:      "",
		Type:       "stdout",
		Plugin:     nil,
		Processors: []processor.Config{},
	}
}

func FromAny(prov docs.Provider, value any) (conf Config, err error) {
	switch t := value.(type) {
	case Config:
		return t, nil
	case *yaml.Node:
		return fromYAML(prov, t)
	case map[string]any:
		return fromMap(prov, t)
	}
	err = fmt.Errorf("unexpected value, expected object, got %T", value)
	return
}

func fromMap(prov docs.Provider, value map[string]any) (conf Config, err error) {
	if conf.Type, _, err = docs.GetInferenceCandidateFromMap(prov, docs.TypeOutput, value); err != nil {
		err = docs.NewLintError(0, docs.LintComponentNotFound, err)
		return
	}

	conf.Label, _ = value["label"].(string)

	if procV, exists := value["processors"]; exists {
		procArr, ok := procV.([]any)
		if !ok {
			err = fmt.Errorf("processors: unexpected value, expected array got %T", procV)
			return
		}
		for i, pv := range procArr {
			var tmpProc processor.Config
			if tmpProc, err = processor.FromAny(prov, pv); err != nil {
				err = fmt.Errorf("%v: %w", i, err)
				return
			}
			conf.Processors = append(conf.Processors, tmpProc)
		}
	}

	if p, exists := value[conf.Type]; exists {
		conf.Plugin = p
	} else if p, exists := value["plugin"]; exists {
		conf.Plugin = p
	}
	return
}

func fromYAML(prov docs.Provider, value *yaml.Node) (conf Config, err error) {
	if conf.Type, _, err = docs.GetInferenceCandidateFromYAML(prov, docs.TypeOutput, value); err != nil {
		err = docs.NewLintError(value.Line, docs.LintComponentNotFound, err)
		return
	}

	for i := 0; i < len(value.Content)-1; i += 2 {
		switch value.Content[i].Value {
		case "label":
			conf.Label = value.Content[i+1].Value
		case "processors":
			for i, n := range value.Content[i+1].Content {
				var tmpProc processor.Config
				if tmpProc, err = processor.FromAny(prov, n); err != nil {
					err = fmt.Errorf("%v: %w", i, err)
					return
				}
				conf.Processors = append(conf.Processors, tmpProc)
			}
		}
	}

	pluginNode, err := docs.GetPluginConfigYAML(conf.Type, value)
	if err != nil {
		err = docs.NewLintError(value.Line, docs.LintFailedRead, err)
		return
	}

	conf.Plugin = &pluginNode
	return
}
